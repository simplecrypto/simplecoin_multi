from flask import current_app
from datetime import datetime
from simpledoge.model_lib import base
from sqlalchemy.schema import CheckConstraint
from cryptokit import bits_to_difficulty

from . import db, coinserv


class Block(base):
    """ This class stores metadata on all blocks found by the pool """
    height = db.Column(db.Integer, primary_key=True)
    # User who discovered block
    user = db.Column(db.String)
    # When block was found
    found_at = db.Column(db.DateTime, default=datetime.utcnow)
    # # Time started on block
    # time_started = db.Column(db.DateTime)
    # Is block now orphaned?
    orphan = db.Column(db.Boolean, default=False)
    # Is the block matured?
    mature = db.Column(db.Boolean, default=False)
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
    def create(cls, user, height, total_value, transaction_fees, bits, hash):
        share = Share.query.order_by(Share.id.desc()).first()
        block = cls(user=user,
                    height=height,
                    total_value=total_value,
                    transaction_fees=transaction_fees,
                    bits=bits,
                    last_share=share,
                    hash=hash)
        # add and flush
        db.session.add(block)
        db.session.flush()
        return block

    @property
    def difficulty(self):
        return bits_to_difficulty(self.bits)


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


class CoinTransaction(base):
    txid = db.Column(db.String, primary_key=True)
    confirmed = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    @classmethod
    def create(cls, txid):
        trans = cls(txid=txid)
        db.session.add(trans)
        return trans

    @classmethod
    def from_serial_transaction(cls, transactions):
        """ Doesn't actually make a cointransaction object, simply creates a
        transaction on the coinserver and returns the new transaction id. 
        Uses the wallet_pass to unlock the wallet on the coinserver and 
        sends the funds from pool_address account. """
        current_app.logger.debug("Setting tx fee: %s" % coinserv.settxfee(1))
        wallet = coinserv.walletpassphrase(
            current_app.config['coinserv']['wallet_pass'], 10)
        current_app.logger.debug("Unlocking wallet: %s" % wallet)
        recip = {r['user']: r['amount'] / float(100000000) for r in transactions}
        current_app.logger.debug("Sending to recip: " + str(recip))
        return coinserv.sendmany(current_app.config['coinserv']['account'],
                                 recip)


class Transaction(base):
    """ An aggregation of payouts that correspond to an actual network
    transaction """
    id = db.Column(db.Integer, primary_key=True)
    user = db.Column(db.String)
    amount = db.Column(db.BigInteger, CheckConstraint('amount>0'))
    sent = db.Column(db.Boolean, default=False)
    txid = db.Column(db.String, db.ForeignKey('coin_transaction.txid'))
    coin_transaction = db.relationship('CoinTransaction')

    @classmethod
    def create(cls, user, amount):
        payout = cls(user=user, amount=amount)
        db.session.add(payout)
        return payout

    @classmethod
    def serialize_pending(cls):
        transactions = Transaction.query.filter_by(coin_transaction=None)
        struct = [{'id': t.id, 'amount': t.amount, 'user': t.user}
                  for t in transactions]
        transactions.update({Transaction.sent: True})
        return struct


class Payout(base):
    """ Represents a users payout for a single round """
    blockheight = db.Column(db.Integer, db.ForeignKey('block.height'),
                            primary_key=True)
    block = db.relationship('Block', foreign_keys=[blockheight])
    user = db.Column(db.String, primary_key=True)
    amount = db.Column(db.BigInteger, CheckConstraint('amount>0'))
    transaction_id = db.Column(db.Integer, db.ForeignKey('transaction.id'))
    transaction = db.relationship('Transaction', foreign_keys=[transaction_id])

    @classmethod
    def create(cls, user, amount, block):
        payout = cls(user=user, amount=amount, block=block)
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
