import calendar
import logging
import json

from datetime import datetime, timedelta
from simpledoge.model_lib import base
from sqlalchemy.schema import CheckConstraint
from sqlalchemy.ext.declarative import AbstractConcreteBase
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
    # the hash of the block for orphan checking
    hash = db.Column(db.String, primary_key=True)
    height = db.Column(db.Integer, nullable=False)
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

    @property
    def status(self):
        if self.mature:
            return "Mature"
        if self.orphan:
            return "Orphan"
        return "Unconfirmed"

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
    """ Retrieves the last time a block was solved using progressively less
    accurate methods. Essentially used to calculate round time. """
    last_block = Block.query.order_by(Block.height.desc()).first()
    if last_block:
        return last_block.found_at

    hour = OneHourShare.query.order_by(OneHourShare.time).first()
    if hour:
        return hour.time

    five = FiveMinuteShare.query.order_by(FiveMinuteShare.time).first()
    if five:
        return five.time

    minute = OneMinuteShare.query.order_by(OneMinuteShare.time).first()
    if minute:
        return minute.time

    return datetime.utcnow()


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


class Status(base):
    """ This class generates a table containing every share accepted for a
    round """
    user = db.Column(db.String, primary_key=True)
    worker = db.Column(db.String, primary_key=True)
    status = db.Column(db.String)
    time = db.Column(db.DateTime)

    @property
    def parsed_status(self):
        return json.loads(self.status)

    def pretty_json(self, gpu=0):
        print type(gpu)
        return json.dumps(json.loads(self.status)['gpus'][gpu], indent=4, sort_keys=True)

    @property
    def stale(self):
        ten_min_ago = datetime.utcnow() - timedelta(minutes=10)
        if ten_min_ago >= self.time:
            return True
        else:
            return False


class Payout(base):
    """ Represents a users payout for a single round """
    id = db.Column(db.Integer, primary_key=True)
    blockhash = db.Column(db.String, db.ForeignKey('block.hash'))
    block = db.relationship('Block', foreign_keys=[blockhash])
    user = db.Column(db.String)
    shares = db.Column(db.BigInteger)
    amount = db.Column(db.BigInteger, CheckConstraint('amount>0', 'min_payout_amount'))
    transaction_id = db.Column(db.String, db.ForeignKey('transaction.txid'))
    transaction = db.relationship('Transaction', foreign_keys=[transaction_id])
    __table_args__ = (
        db.UniqueConstraint("user", "blockhash"),
    )

    @classmethod
    def create(cls, user, amount, block, shares):
        payout = cls(user=user, amount=amount, block=block, shares=shares)
        db.session.add(payout)
        return payout


class TimeSlice(AbstractConcreteBase, base):
    user = db.Column(db.String, primary_key=True)
    # datetime floored to the minute
    time = db.Column(db.DateTime, primary_key=True)
    worker = db.Column(db.String, primary_key=True)
    value = db.Column(db.Integer)

    @classmethod
    def create(cls, user, value, time, worker):
        dt = cls.floor_time(time)
        slc = cls(user=user, value=value, time=dt, worker=worker)
        db.session.add(slc)
        return slc

    @classmethod
    def add_value(cls, user, value, time, worker):
        dt = cls.floor_time(time)
        slc = cls.query.with_lockmode('update').filter_by(
            user=user, time=dt, worker=worker).one()
        slc.value += value

    @classmethod
    def floor_time(cls, time):
        """ Changes an integer timestamp to the minute for which it falls in.
        Allows abstraction of create and add share logic for each time slice
        object. """
        if isinstance(time, datetime):
            time = calendar.timegm(time.utctimetuple())
        return datetime.utcfromtimestamp(
            (time // cls.slice_seconds) * cls.slice_seconds)

    @classmethod
    def compress(cls):
        """ Moves statistics that are past the `window` time into the next
        time slice size, effectively compressing the data. """
        # get the minute shares that are old enough to be compressed and
        # deleted
        recent = cls.floor_time(datetime.utcnow()) - cls.window
        # the five minute slice currently being processed
        current_slice = None
        # dictionary of lists keyed by user
        users = {}

        def create_upper():
            # add a time slice for each user in this pending period
            for (user, worker), slices in users.iteritems():
                total = sum([slc.value for slc in slices])

                # put it in the database
                upper = cls.upper.query.filter_by(user=user, worker=worker, time=current_slice).with_lockmode('update').first()
                # wasn't in the db? create it
                if not upper:
                    upper = cls.upper.create(user, total, current_slice, worker)
                # was in the db? add the shares for each worker, and
                # increment total share count
                else:
                    upper.value += total

                for slc in slices:
                    db.session.delete(slc)

        # traverse minute shares that are old enough in time order
        for slc in (cls.query.filter(cls.time < recent).
                    order_by(cls.time)):
            slice_time = cls.upper.floor_time(slc.time)

            if current_slice is None:
                current_slice = slice_time

            # we've encountered the next time slice, so commit the pending one
            if slice_time != current_slice:
                logging.debug("Processing slice " + str(current_slice))
                create_upper()
                users.clear()
                current_slice = slice_time

            # add the one min shares for this user the list of pending shares
            # to be grouped together
            key = (slc.user, slc.worker)
            users.setdefault(key, [])
            users[key].append(slc)

        create_upper()


class OneHourShare(TimeSlice):
    __tablename__ = 'one_hour_share'
    window = timedelta(days=30)
    slice = timedelta(hours=1)
    slice_seconds = slice.total_seconds()
    __mapper_args__ = {
        'polymorphic_identity': 'one_hour_share',
        'concrete': True
    }


class FiveMinuteShare(TimeSlice):
    __tablename__ = 'five_minute_share'
    window = timedelta(days=1)
    slice = timedelta(minutes=5)
    slice_seconds = slice.total_seconds()
    upper = OneHourShare
    __mapper_args__ = {
        'polymorphic_identity': 'five_minute_share',
        'concrete': True
    }


class OneMinuteShare(TimeSlice):
    __tablename__ = 'one_minute_share'
    window = timedelta(hours=1)
    slice = timedelta(minutes=1)
    slice_seconds = slice.total_seconds()
    upper = FiveMinuteShare
    __mapper_args__ = {
        'polymorphic_identity': 'one_minute_share',
        'concrete': True
    }
