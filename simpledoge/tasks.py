from celery import Celery
from simpledoge import db
from simpledoge.models import Share, Block, OneMinuteShare
from datetime import datetime
from cryptokit import bits_to_shares

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
def cleanup(self):
    """
    Finds all the shares that will no longer be used and removes them from
    the database.
    """
    try:
        # find the oldest un-processed block
        block = Block.query.filter_by(processed=False).order_by(Block.height).first()
        if block is None:
            block = Block.query.order_by(Block.height).first()
            if block is None:
                return

        #minute = (minute // 60) * 60
        #OneMinuteShare.create(user, shares, datetime.fromtimestamp(minute))
        #db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in add_one_minute", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)
