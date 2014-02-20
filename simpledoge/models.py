from datetime import datetime
from . import db
from simpledoge.model_lib import base


class Block(base):
    """ This class stores metadata on all blocks found by the pool """
    height = db.Column(db.Integer, primary_key=True)
    # User who discovered block
    user = db.Column(db.String)
    found_at = db.Column(db.DateTime, default=datetime.utcnow())
    # Is block now orphaned?
    orphan = db.Column(db.Boolean)
    # Block value (does not include transaction fees recieved)
    total_value = db.Column(db.BigInteger)
    # Associated transaction fees
    transaction_fees = db.Column(db.BigInteger)
    # total going to pool from fees
    fees = db.Column(db.BigInteger)
    bits = db.Column(db.String(8))
    # have payments been generated for it?
    processed = db.Column(db.Boolean)

    @classmethod
    def create(cls, user, height, total_value, transaction_fees, bits):
        block = cls(user=user,
                    height=height,
                    total_value=total_value,
                    transaction_fees=transaction_fees,
                    bits=bits)
        # add and flush
        db.session.add(block)
        db.session.flush()
        return block

    def get_last_share(self):
        """ Fetches the last share that was entered directly before this block
        """
        return (Share.query.
                filter(Share.found_at < self.found_at).
                order_by(Share.found_at).first())


class Share(base):
    """ This class generates a table containing every share accepted for a
    round """
    id = db.Column(db.Integer, primary_key=True)
    user = db.Column(db.String)
    shares = db.Column(db.Integer)
    found_at = db.Column(db.DateTime, default=datetime.utcnow())

    @classmethod
    def create(cls, user, shares):
        share = cls(user=user, shares=shares)
        db.session.add(share)
        return share


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
