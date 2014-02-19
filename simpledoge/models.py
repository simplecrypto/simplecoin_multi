from datetime import datetime
from diff_match_patch import diff_match_patch
from . import db
from simpledoge.model_lib import base

dmp = diff_match_patch()


class User(base):
    """ This class stores all users metadata and financial info"""
    # User's wallet address
    username = db.Column(db.String, primary_key=True)
    # User creation time
    created_at = db.Column(db.DateTime, default=datetime.utcnow())

    ## User Financial Data ##
    # confirmed balance
    confirmed_balance = db.Column(db.Integer, default=0)
    # unconfirmed balance
    unconfirmed_balance = db.Column(db.Integer, default=0)
    # total every paid out
    ever_paid_out = db.Column(db.Integer, default=0)

    @classmethod
    def create(cls, username):
        user = cls(username=username)
        # add and flush
        db.session.add(user)
        db.session.flush()
        return user


class Block(base):
    """ This class stores metadata on all blocks found by the pool """
    id = db.Column(db.Integer, primary_key=True)
    # User who discovered block
    found_by = db.Column(db.String)
    # Blockheight
    blockheight = db.Column(db.Integer)
    # Datetime block discovered
    found_at = db.Column(db.DateTime, default=datetime.utcnow())
    # Is block orphaned?
    orphan = db.Column(db.Boolean)
    # Block value (does not include transaction fees recieved)
    network_value = db.Column(db.Integer)
    # Associated transaction fees
    transaction_fees = db.Column(db.Integer)

    @classmethod
    def create(cls):
        block = cls()
        # add and flush
        db.session.add(block)
        db.session.flush()
        return block

    def complete(self, found_by, blockheight, network_value, transaction_fees):
        self.found_by = found_by
        self.blockheight = blockheight
        self.network_value = network_value
        self.transaction_fees = transaction_fees
        # flush
        db.session.flush()
        return True

class RoundShare(base):
    """ This class generates a table containing every share accepted for a round """
    id = db.Column(db.Integer, primary_key=True)
    for_block = db.Column(db.Integer, db.ForeignKey('block.id'))
    found_by = db.Column(db.String, db.ForeignKey('user.username'))
    share_value = db.Column(db.Integer)
    found_at = db.Column(db.DateTime, default=datetime.utcnow())

    @classmethod
    def create(cls, for_block, found_by, share_value):
        share = cls(for_block=for_block,
                    found_by=found_by,
                    share_value=share_value)
        # add, but don't flush here
        db.session.add(share)
        return True

class OneMinuteShare(base):
    """ This class stores a users accepted n1 shares for a 1min period.
     This data is generated from summarizing data in the round shares table. """
    id = db.Column(db.Integer, primary_key=True)
    for_block = db.Column(db.Integer, db.ForeignKey('block.id'))
    found_by = db.Column(db.String, db.ForeignKey('user.username'))
    # n1 share count for the minute
    n1_shares = db.Column(db.Integer)
    # datetime floored to the minute
    minute = db.Column(db.DateTime)

    @classmethod
    def create(cls, for_block, found_by, n1_shares, minute):
        minute_shares = cls(for_block=for_block,
                            found_by=found_by,
                            n1_shares=n1_shares,
                            minute=minute)
        # add, but don't flush here
        db.session.add(minute_shares)
        return True

class FiveMinuteShare(base):
    """ This class stores a users accepted n1 shares for a 5min period.
     This data is generated from summarizing data in the 1min table. """
    id = db.Column(db.Integer, primary_key=True)
    found_by = db.Column(db.String, db.ForeignKey('users.username'))
    # n1 share count for 5 minutes
    n1_shares = db.Column(db.Integer)
    # datetime floored to a minute representing the 5min period
    minute = db.Column(db.DateTime)

    @classmethod
    def create(cls, found_by, n1_shares, minute):
        five_minute_shares = cls(found_by=found_by,
                                 n1_shares=n1_shares,
                                 minute=minute)
        # add, but don't flush here
        db.session.add(five_minute_shares)
        return True