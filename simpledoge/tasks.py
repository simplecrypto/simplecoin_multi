from flask import current_app
from celery import Celery
from simpledoge import db, coinserv
from simpledoge.models import Share, Block, OneMinuteShare, Payout, Transaction, CoinTransaction
from datetime import datetime
from cryptokit import bits_to_shares
from pprint import pformat
from bitcoinrpc.proxy import JSONRPCException

import logging

logger = logging.getLogger('tasks')
celery = Celery('simpledoge')


@celery.task(bind=True)
def update_coin_transaction(self):
    """
    Loops through all immature transactions
    """
    try:
        # Select all unconfirmed transactions
        unconfirmed = (CoinTransaction.query.filter_by(confirmed=False))
        blockheight = coinserv.getblockcount()
        for tx in unconfirmed:
            # Check to see if the transaction hash exists in the block chain
            try:
                t = coinserv.gettransaction(tx.id)
                if t.get('confirmations', 0) >= 6:
                    tx.confirmed = True
            except JSONRPCException:
                tx.confirmed = False

        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in update block status", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def update_block_state(self):
    """
    Loops through all immature and non-orphaned blocks.
    First checks to see if blocks are orphaned,
    then it checks to see if they are now matured.
    """
    try:
        # Select all immature & non-orphaned blocks
        immature = (Block.query.filter_by(mature=False, orphan=False))
        blockheight = coinserv.getblockcount()
        for block in immature:
            # Check to see if the block hash exists in the block chain
            try:
                coinserv.getblock(block.hash)
            except JSONRPCException:
                block.orphan = True
            # Check to see if the block is matured & not orphaned
            if blockheight-120 > block.height & block.orphan is False:
                block.mature = True

        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in update block status", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


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
def new_block(self, blockheight):
    """
    Notification that a new block height has been reached.
    """
    try:
        (Payout.query.
         filter(Payout.block <= (blockheight - 120)).
         update({Payout.mature: True}))
        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in new_block", exc_info=True)
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
                logger.debug("No block found, exiting...")
                return

        mult = int(current_app.config['last_n'])
        # take our standard share count times two for a safe margin. divide
        # by 16 to get the number of rows, since a rows minimum share count
        # is 16
        shares = (bits_to_shares(block.bits) * mult) * 2
        id_diff = shares // 16
        # compute the id which earlier ones are safe to delete
        stale_id = block.last_share.id - id_diff
        if simulate:
            print("Share for block computed: {}".format(shares // 2))
            print("Share total margin computed: {}".format(shares))
            print("Id diff computed: {}".format(id_diff))
            print("Stale ID computed: {}".format(stale_id))
            exit(0)
        elif stale_id > 0:
            logger.info("Cleaning all shares older than {}".format(stale_id))
            # delete all shares that are sufficiently old
            Share.query.filter(Share.id < stale_id).delete(
                synchronize_session=False)
            db.session.commit()
        else:
            logger.info("Not cleaning anything, stale id less than zero")
    except Exception as exc:
        logger.error("Unhandled exception in cleanup", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def gen_transactions(self):
    minimum_payout = current_app.config['minimum_payout']
    #shares = (db.session.query(Payout.user, Payout.block, func.sum(Payout.amount)).
    #          filter(Payout.transaction_id == None).
    #          join(Payout.block, aliased=True).
    #          group_by(Payout.user).
    #          having(func.sum(Payout.amount) > minimum_payout).all())
    payouts = (Payout.query.filter_by(transaction=None).
               join(Payout.block, aliased=True).filter_by(mature=True))

    users = {}
    for payout in payouts:
        users.setdefault(payout.user, {'amount': 0, 'payouts': []})
        users[payout.user]['amount'] += payout.amount
        users[payout.user]['payouts'].append(payout)

    for user, vals in users.iteritems():
        if vals['amount'] < minimum_payout:
            continue

        # create a new transaction for the user
        transaction = Transaction.create(user, vals['amount'])
        # link the payouts to it
        for payout in vals['payouts']:
            payout.transaction = transaction

    db.session.commit()


@celery.task(bind=True)
def payout(self, simulate=False):
    """
    Calculates payouts for users from share records for found blocks.
    """
    try:
        if simulate:
            logger.setLevel(logging.DEBUG)

        # find the oldest un-processed block
        block = (Block.query.
                 filter_by(processed=False).
                 order_by(Block.height).first())
        if block is None:
            logger.debug("No block found, exiting...")
            return

        logger.debug("Processing block height {}".format(block.height))

        mult = int(current_app.config['last_n'])
        # take our standard share count times two for a safe margin. divide
        # by 16 to get the number of rows, since a rows minimum share count
        # is 16
        total_shares = (bits_to_shares(block.bits) * mult)
        logger.debug("Looking for up to {} total shares".format(total_shares))
        remain = total_shares
        start = block.last_share.id
        logger.debug("Identified last matching share id as {}".format(start))
        user_shares = {}
        for share in Share.query.filter(Share.id <= start).yield_per(100):
            user_shares.setdefault(share.user, 0)
            if remain > share.shares:
                user_shares[share.user] += share.shares
                remain -= share.shares
            else:
                user_shares[share.user] += remain
                remain = 0
                break

        # if we found less than n, use what we found as the total
        total_shares -= remain
        logger.debug("Found {} shares".format(total_shares))
        if simulate:
            logger.debug("Share distribution:\n {}"
                         .format(pformat(user_shares)))

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
                     .format(accrued, (accrued / float(distribute_amnt)) * 100))
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
        logger.debug("Successfully distributed all fees among {} users"
                     .format(len(user_shares)))

        if simulate:
            logger.debug("Share distribution:\n {}".format(pformat(user_shares)))
            db.session.rollback()
        else:
            db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in payout", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)
