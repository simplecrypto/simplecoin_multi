from flask import current_app
from celery import Celery
from simpledoge import db
from simpledoge.models import Share, Block, OneMinuteShare
from datetime import datetime
from cryptokit import bits_to_shares
from pprint import pformat

import logging

logger = logging.getLogger('tasks')
celery = Celery('simpledoge')


@celery.task(bind=True)
def add_share(self, user, shares):
    """
    Adds a round share to postgresql

    user: should be a username/wallet address
    shares: should be an integer representation of n1 shares
    """
    try:
        Share.create(user=user, shares=shares)
        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in add share", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def add_block(self, user, height, total_value, transaction_fees, bits):
    """
    Insert postgresql a block & blockchain data

    user: should be a username/wallet address of who found block
    height: should be the height of the given block in the blockchain
    total_value: should be an integer representation of the value of the
                  newly discovered block. E.G.
                  DOGE = 2364681.04976814
                  network_value = 236468104976814

    transaction_fees: should be an integer amount awarded due to transactions
                     handled by the block. E.G.
                     transaction fees on new block = 6.5
                     transaction_fees = 650000000
    """
    try:
        Block.create(user, height, total_value, transaction_fees, bits)
        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in add_block", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def add_one_minute(self, user, shares, minute):
    """
    Adds a new single minute entry for a user

    minute: timestamp (int)
    shares: number of shares recieved over the timespan
    user: string of the user
    """
    try:
        minute = (minute // 60) * 60
        OneMinuteShare.create(user, shares, datetime.fromtimestamp(minute))
        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in add_one_minute", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def cleanup(self, simulate=False):
    """
    Finds all the shares that will no longer be used and removes them from
    the database.
    """
    try:
        # find the oldest un-processed block
        block = (Block.query.
                 filter_by(processed=False).
                 order_by(Block.height).first())
        if block is None:
            # sloppy hack to get the newest block that is processed
            block = (Block.query.
                     filter_by(processed=True).
                     order_by(Block.height.desc()).first())
            if block is None:
                return

        mult = int(current_app.config['last_n'])
        # take our standard share count times two for a safe margin. divide
        # by 16 to get the number of rows, since a rows minimum share count
        # is 16
        shares = (bits_to_shares(block.bits) * mult) * 2
        id_diff = shares // 16
        # compute the id which earlier ones are safe to delete
        stale_id = block.get_last_share().id - id_diff
        if simulate:
            print("Share for block computed: {}".format(shares // 2))
            print("Share total margin computed: {}".format(shares))
            print("Id diff computed: {}".format(id_diff))
            print("Stale ID computed: {}".format(stale_id))
            exit(0)
        else:
            # delete all shares that are sufficiently old
            Share.query.filter(Share.id < stale_id).delete(
                synchronize_session=False)
            db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in cleanup", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def payout(self, simulate=False):
    """
    Calculates payouts for users from share records for found blocks.
    """
    from sqlalchemy.orm import load_only
    try:
        if simulate:
            logger.setLevel(logging.DEBUG)

        # find the oldest un-processed block
        block = (Block.query.
                 filter_by(processed=False).
                 order_by(Block.height).first())
        if block is None:
            return
        logger.debug("Processing block id {}".format(block.id))

        mult = int(current_app.config['last_n'])
        # take our standard share count times two for a safe margin. divide
        # by 16 to get the number of rows, since a rows minimum share count
        # is 16
        total_shares = (bits_to_shares(block.bits) * mult)
        remain = total_shares
        start = block.get_last_share().id
        logger.debug("Identified last matching share id as {}".format(start))
        user_shares = {}
        for share in Share.query.filter(Share.id <= start).options(
                load_only("shares", "user")).yield_per(100):
            user_shares.setdefault(share.user, 0)
            if remain > share.shares:
                user_shares[share.user] += share.shares
                remain -= share.shares
            else:
                user_shares[share.user] += remain
                remain = 0
                break

        # Calculate the portion going to the miners by truncating the
        # fractional portions and giving the remainder to the pool owner
        fee = float(current_app.config['fee'])
        logger.debug("Fee loaded as {}".format(fee))
        distribute_amnt = int((block.total_value * (100 - fee)) // 100)
        # record the fee collected by the pool for convenience
        block.fees = block.total_value - distribute_amnt
        logger.debug("Block fees computed to {}".format(block.fees))
        logger.debug("Distribute_amnt: {}".format(distribute_amnt))
        # Below calculates the truncated portion going to each miner. because
        # of fractional pieces the total accrued wont equal the disitrubte_amnt
        # so we will distribute that round robin the miners in dictionary order
        accrued = 0
        # somewhat risky, but we're going to modify the dictoinary in place
        # for minor memory concerns...
        for user, share_count in user_shares.iteritems():
            user_shares[user] = (share_count * distribute_amnt) // total_shares
            accrued += user_shares[user]

        logger.debug("Total accrued after trunated iteration {}; {}%"
                     .format(accrued, (accrued / distribute_amnt) * 100))
        # loop over the dictionary indefinitely until we've distributed
        # all the remaining funds
        while accrued < distribute_amnt:
            for key in user_shares:
                user_shares[key] += 1
                accrued += 1
                # exit if we've exhausted
                if accrued >= distribute_amnt:
                    break

        assert accrued == distribute_amnt

        if simulate:
            logger.debug("Share distribution: {}".format(pformat(user_shares)))
            db.session.rollback()
        else:
            db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in cleanup", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)
