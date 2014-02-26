from datetime import datetime, timedelta
from simpledoge.model_lib import base
from sqlalchemy.schema import CheckConstraint
from sqlalchemy.dialects.postgresql import HSTORE
from cryptokit import bits_to_difficulty

from . import db


class Blob(base):
    """ Used to store misc single value blobs of data, such as the current
    block height and difficulty. """
    key = db.Column(db.String, primary_key=True)
    data = db.Column(HSTORE, default=dict)


class Block(base):
    """ This class stores metadata on all blocks found by the pool """
    height = db.Column(db.Integer, primary_key=True)
    # User who discovered block
    user = db.Column(db.String)
    # When block was found
    found_at = db.Column(db.DateTime, default=datetime.utcnow)
    # # Time started on block
    time_started = db.Column(db.DateTime, nullable=False)
    # Is block now orphaned?
    orphan = db.Column(db.Boolean, default=False)
    # Is the block matured?
    mature = db.Column(db.Boolean, default=False)
    # Total shares that were required to solve the block
    shares_to_solve = db.Column(db.BigInteger)
    # Block value (does not include transaction fees recieved)
    total_value = db.Column(db.BigInteger)
    # Associated transaction fees
    transaction_fees = db.Column(db.BigInteger)
    # total going to pool from fees
    fees = db.Column(db.BigInteger)
    # Difficulty of block when solved
    bits = db.Column(db.String(8), nullable=False)
    # the last share id that was processed when the block was entered.
    # used as a marker for calculating last n shares
    last_share_id = db.Column(db.BigInteger, db.ForeignKey('share.id'))
    last_share = db.relationship('Share', foreign_keys=[last_share_id])
    # have payments been generated for it?
    processed = db.Column(db.Boolean, default=False)
    # the hash of the block for orphan checking
    hash = db.Column(db.String, nullable=False)

    @classmethod
    def create(cls, user, height, total_value, transaction_fees, bits, hash,
               time_started):
        share = Share.query.order_by(Share.id.desc()).first()
        block = cls(user=user,
                    height=height,
                    total_value=total_value,
                    transaction_fees=transaction_fees,
                    bits=bits,
                    last_share=share,
                    hash=hash,
                    time_started=time_started)
        # add and flush
        db.session.add(block)
        db.session.flush()
        return block

    @property
    def difficulty(self):
        return bits_to_difficulty(self.bits)

    @property
    def duration(self):
        seconds = round((self.found_at - self.time_started).total_seconds())
        formatted_time = str(timedelta(seconds=seconds))
        return formatted_time


def last_block_time():
    last_block = Block.query.order_by(Block.height.desc()).first()
    if not last_block:
        first_min_share = OneMinuteShare.order_by(OneMinuteShare.minute.desc()).query.first()
        if first_min_share:
            return first_min_share.minute
        else:
            return datetime.utcnow()
    else:
        return last_block.found_at


def last_block_share_id():
    last_block = Block.query.order_by(Block.height.desc()).first()
    if not last_block:
        return 0
    return last_block.last_share_id


class Share(base):
    """ This class generates a table containing every share accepted for a
    round """
    id = db.Column(db.BigInteger, primary_key=True)
    user = db.Column(db.String)
    shares = db.Column(db.Integer)

    @classmethod
    def create(cls, user, shares):
        share = cls(user=user, shares=shares)
        db.session.add(share)
        return share


class Transaction(base):
    txid = db.Column(db.String, primary_key=True)
    confirmed = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    @classmethod
    def create(cls, txid):
        trans = cls(txid=txid)
        db.session.add(trans)
        return trans


class Payout(base):
    """ Represents a users payout for a single round """
    id = db.Column(db.Integer, primary_key=True)
    blockheight = db.Column(db.Integer, db.ForeignKey('block.height'))
    block = db.relationship('Block', foreign_keys=[blockheight])
    user = db.Column(db.String)
    shares = db.Column(db.BigInteger)
    amount = db.Column(db.BigInteger, CheckConstraint('amount>0'))
    transaction_id = db.Column(db.String, db.ForeignKey('transaction.txid'))
    transaction = db.relationship('Transaction', foreign_keys=[transaction_id])
    __table_args__ = (
        db.UniqueConstraint("user", "blockheight"),
    )

    @classmethod
    def create(cls, user, amount, block, shares):
        payout = cls(user=user, amount=amount, block=block, shares=shares)
        db.session.add(payout)
        return payout


class OneMinuteShare(base):
    """ This class stores a users accepted n1 shares for a 1min period.  This
    data is generated from summarizing data in the round shares table. """
    user = db.Column(db.String, primary_key=True)
    # datetime floored to the minute
    minute = db.Column(db.DateTime, primary_key=True)
    # n1 share count for the minute
    shares = db.Column(db.Integer)

    @classmethod
    def create(cls, user, shares, minute):
        minute_share = cls(user=user,
                           shares=shares,
                           minute=minute)
        db.session.add(minute_share)
        return minute_share
