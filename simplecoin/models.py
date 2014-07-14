import calendar
import logging

from math import ceil, floor
from collections import namedtuple
from datetime import datetime, timedelta
from flask import current_app
from sqlalchemy.schema import CheckConstraint
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from cryptokit import bits_to_difficulty

from .model_lib import base
from . import db, cache, sig_round


class Block(base):
    """ This class stores metadata on all blocks found by the pool """
    # the hash of the block for orphan checking
    hash = db.Column(db.String, primary_key=True)
    height = db.Column(db.Integer, nullable=False)
    # User who discovered block
    user = db.Column(db.String)
    worker = db.Column(db.String)
    # When block was found
    found_at = db.Column(db.DateTime, nullable=False)
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
    donated = db.Column(db.BigInteger)
    bonus_payed = db.Column(db.BigInteger)
    # Difficulty of block when solved
    bits = db.Column(db.String(8), nullable=False)
    currency = db.Column(db.String, nullable=False)
    merged_type = db.Column(db.String)
    algo = db.Column(db.String, nullable=False)

    standard_join = ['status', 'explorer_link', 'luck', 'total_value_float',
                     'difficulty', 'duration', 'found_at', 'time_started']

    @property
    def status(self):
        if self.mature:
            return "Mature"
        if self.orphan:
            return "Orphan"
        confirms = self.confirms_remaining
        if confirms is not None:
            return "{} Confirms Remaining".format(confirms)
        else:
            return "Pending confirmation"

    @classmethod
    def create(cls, user, height, total_value, transaction_fees, bits, hash,
               time_started, currency, worker, found_at, algo, merged_type=None):
        block = cls(user=user,
                    height=height,
                    total_value=total_value,
                    transaction_fees=transaction_fees,
                    bits=bits,
                    hash=hash,
                    time_started=time_started,
                    algo=algo,
                    merged_type=merged_type,
                    currency=currency,
                    found_at=found_at,
                    worker=worker)
        # add and flush
        db.session.add(block)
        return block

    @property
    def explorer_link(self):
        if not self.merged_type:
            return current_app.config['block_link_prefix'] + self.hash
        else:
            cfg = current_app.config['merged_cfg'][self.merged_type]
            return cfg['block_link_prefix'] + self.hash

    @property
    def luck(self):
        return ((self.difficulty * (2 ** 16) / current_app.config.get('share_multiplier', 1)) / self.shares_to_solve) * 100

    @property
    def total_value_float(self):
        return self.total_value / 100000000.0

    @property
    def difficulty(self):
        return bits_to_difficulty(self.bits)

    @property
    def timestamp(self):
        return calendar.timegm(self.found_at.utctimetuple())

    @property
    def duration(self):
        seconds = round((self.found_at - self.time_started).total_seconds())
        formatted_time = timedelta(seconds=seconds)
        return formatted_time

    @property
    def confirms_remaining(self):
        bh = cache.get('blockheight')
        if not bh or self.merged_type:
            return None
        confirms_req = current_app.config['block_mature_confirms']
        # prevent displaying negative confirms
        return max(0, confirms_req - (bh - self.height))


class Transaction(base):
    txid = db.Column(db.String, primary_key=True)
    confirmed = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    merged_type = db.Column(db.String)

    @classmethod
    def create(cls, txid, merged_type=None):
        trans = cls(txid=txid, merged_type=merged_type)
        db.session.add(trans)
        return trans


class Payout(base):
    blockhash = db.Column(db.String, db.ForeignKey('block.hash'), primary_key=True)
    block = db.relationship('Block', foreign_keys=[blockhash])
    user = db.Column(db.String, primary_key=True)
    amount = db.Column(db.BigInteger, CheckConstraint('amount > 0', 'min_payout_amount'))

    locked = db.Column(db.Boolean, default=False)
    shares = db.Column(db.Integer)
    perc = db.Column(db.Float)
    transaction = db.relationship('Transaction', backref='payouts')
    transaction_id = db.Column(db.String, db.ForeignKey('transaction.txid'))

    standard_join = ['status', 'created_at', 'explorer_link',
                     'text_perc_applied', 'mined', 'amount_float', 'height',
                     'transaction_id']

    @property
    def amount_float(self):
        return self.amount / 100000000.0

    @property
    def perc_applied(self):
        if self.perc >= 0:
            return int(ceil((self.perc / 100.0) * self.amount))
        return int(floor((self.perc / 100.0) * self.amount))

    @property
    def text_perc_applied(self):
        if self.perc < 0:
            return "bonus of {}".format(sig_round(self.perc_applied * -1 / 100000000.0))
        else:
            return "donation of {}".format(sig_round(self.perc_applied / 100000000.0))

    @property
    def mined(self):
        return (self.amount + self.perc_applied) / 100000000.0

    @classmethod
    def create(cls, user, amount, block, shares, perc):
        payout = cls(user=user, amount=amount, block=block, shares=shares, perc=perc)
        db.session.add(payout)
        return payout

    @property
    def height(self):
        return self.block.height

    @property
    def status(self):
        if self.transaction:
            if self.transaction.confirmed is True:
                return "Payout Transaction Confirmed"
            else:
                return "Payout Transaction Pending"
        elif self.block.orphan:
            return "Block Orphaned"
        elif not self.block.mature:

            confirms = self.block.confirms_remaining
            if confirms is not None:
                return "{} Block Confirms Remaining".format(confirms)
            else:
                return "Pending Block Confirmation"
        else:
            return "Payout Pending"


class TransactionSummary(base):
    transaction_id = db.Column(db.String, db.ForeignKey('transaction.txid'), primary_key=True)
    transaction = db.relationship('Transaction', backref='summaries')
    user = db.Column(db.String, primary_key=True)
    amount = db.Column(db.BigInteger)
    count = db.Column(db.SmallInteger)

    @classmethod
    def create(cls, txid, user, amount, count=None):
        trans = cls(transaction_id=txid, user=user, amount=amount, count=count)
        db.session.add(trans)
        return trans

    __table_args__ = (
        db.Index('summary_user_idx', 'user'),
    )


class DonationPercent(base):
    user = db.Column(db.String, primary_key=True)
    perc = db.Column(db.Float)


class SliceMixin(object):
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
            for key, slices in users.iteritems():
                new_val = cls.combine(*[slc.value for slc in slices])

                # put it in the database
                upper = cls.upper.query.filter_by(time=current_slice, **key._asdict()).with_lockmode('update').first()
                # wasn't in the db? create it
                if not upper:
                    dt = cls.floor_time(current_slice)
                    upper = cls.upper(time=dt, value=new_val, **key._asdict())
                    db.session.add(upper)
                else:
                    upper.value = cls.combine(upper.value, new_val)

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
            key = slc.make_key()
            users.setdefault(key, [])
            users[key].append(slc)

        create_upper()


@classmethod
def average_combine(cls, *lst):
    """ Takes an iterable and combines the values. Usually either returns
    an average or a sum. Can assume at least one item in list """
    return sum(lst) / len(lst)


@classmethod
def sum_combine(cls, *lst):
    """ Takes a query list and combines the values. Usually either returns
    an average or a sum. Can assume at least one item in ql """
    return sum(lst)


class WorkerTimeSlice(AbstractConcreteBase, SliceMixin, base):
    """ An time abstracted data sample that pertains to a single worker.
    Currently used to represent accepted and rejected shares. """
    user = db.Column(db.String, primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    worker = db.Column(db.String, primary_key=True)
    value = db.Column(db.Float)

    combine = sum_combine
    key = namedtuple('Key', ['user', 'worker'])

    def make_key(self):
        return self.key(user=self.user, worker=self.worker)


class DeviceTimeSlice(AbstractConcreteBase, SliceMixin, base):
    """ An time abstracted data sample that pertains to a single workers single
    device.  Currently used to temperature and hashrate. """
    user = db.Column(db.String, primary_key=True)
    device = db.Column(db.Integer, primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    worker = db.Column(db.String, primary_key=True)
    value = db.Column(db.Integer)

    combine = average_combine
    key = namedtuple('Key', ['user', 'worker', 'device'])

    def make_key(self):
        return self.key(user=self.user, worker=self.worker, device=self.device)


class TypeTimeSlice(AbstractConcreteBase, SliceMixin, base):
    """ An time abstracted data sample that pertains to a single workers single
    device.  Currently used to temperature and hashrate. """
    typ = db.Column(db.String, primary_key=True)
    time = db.Column(db.DateTime, primary_key=True)
    value = db.Column(db.Integer)

    combine = average_combine
    key = namedtuple('Key', ['typ'])

    def make_key(self):
        return self.key(typ=self.typ)


# Mixin classes the define time windows of generic timeslices
class OneMinute(object):
    window = timedelta(hours=1)
    slice = timedelta(minutes=1)
    slice_seconds = slice.total_seconds()


class OneHour(object):
    window = timedelta(days=30)
    slice = timedelta(hours=1)
    slice_seconds = slice.total_seconds()


class FiveMinute(object):
    window = timedelta(days=1)
    slice = timedelta(minutes=5)
    slice_seconds = slice.total_seconds()


# All of our accepted share timeslices
class OneHourShare(WorkerTimeSlice, OneHour):
    __tablename__ = 'one_hour_share'
    __mapper_args__ = {
        'polymorphic_identity': 'one_hour_share',
        'concrete': True
    }


class FiveMinuteShare(WorkerTimeSlice, FiveMinute):
    __tablename__ = 'five_minute_share'
    upper = OneHourShare
    __mapper_args__ = {
        'polymorphic_identity': 'five_minute_share',
        'concrete': True
    }


class OneMinuteShare(WorkerTimeSlice, OneMinute):
    __tablename__ = 'one_minute_share'
    upper = FiveMinuteShare
    __mapper_args__ = {
        'polymorphic_identity': 'one_minute_share',
        'concrete': True
    }


# All of our reject time slices
class OneHourReject(WorkerTimeSlice, OneHour):
    __tablename__ = 'one_hour_reject'
    __mapper_args__ = {
        'polymorphic_identity': 'one_hour_reject',
        'concrete': True
    }


class FiveMinuteReject(WorkerTimeSlice, FiveMinute):
    __tablename__ = 'five_minute_reject'
    upper = OneHourReject
    __mapper_args__ = {
        'polymorphic_identity': 'five_minute_reject',
        'concrete': True
    }


class OneMinuteReject(WorkerTimeSlice, OneMinute):
    __tablename__ = 'one_minute_reject'
    upper = FiveMinuteReject
    __mapper_args__ = {
        'polymorphic_identity': 'one_minute_reject',
        'concrete': True
    }


# Temperature time slices
class OneHourTemperature(DeviceTimeSlice, OneHour):
    __tablename__ = 'one_hour_temperature'
    __mapper_args__ = {
        'polymorphic_identity': 'one_hour_temperature',
        'concrete': True
    }


class FiveMinuteTemperature(DeviceTimeSlice, FiveMinute):
    __tablename__ = 'five_minute_temperature'
    upper = OneHourTemperature
    __mapper_args__ = {
        'polymorphic_identity': 'five_minute_temperature',
        'concrete': True
    }


class OneMinuteTemperature(DeviceTimeSlice, OneMinute):
    __tablename__ = 'one_minute_temperature'
    upper = FiveMinuteTemperature
    __mapper_args__ = {
        'polymorphic_identity': 'one_minute_temperature',
        'concrete': True
    }


# Hashrate timeslices
class OneHourHashrate(DeviceTimeSlice, OneHour):
    __tablename__ = 'one_hour_hashrate'
    __mapper_args__ = {
        'polymorphic_identity': 'one_hour_hashrate',
        'concrete': True
    }


class FiveMinuteHashrate(DeviceTimeSlice, FiveMinute):
    __tablename__ = 'five_minute_hashrate'
    upper = OneHourHashrate
    __mapper_args__ = {
        'polymorphic_identity': 'five_minute_hashrate',
        'concrete': True
    }


class OneMinuteHashrate(DeviceTimeSlice, OneMinute):
    __tablename__ = 'one_minute_hashrate'
    upper = FiveMinuteHashrate
    __mapper_args__ = {
        'polymorphic_identity': 'one_minute_hashrate',
        'concrete': True
    }


# Pool global attributes split up by type, such as worker count
class OneHourType(TypeTimeSlice, OneHour):
    __tablename__ = 'one_hour_type'
    __mapper_args__ = {
        'polymorphic_identity': 'one_hour_type',
        'concrete': True
    }


class FiveMinuteType(TypeTimeSlice, FiveMinute):
    __tablename__ = 'five_minute_type'
    upper = OneHourType
    __mapper_args__ = {
        'polymorphic_identity': 'five_minute_type',
        'concrete': True
    }


class OneMinuteType(TypeTimeSlice, OneMinute):
    __tablename__ = 'one_minute_type'
    upper = FiveMinuteType
    __mapper_args__ = {
        'polymorphic_identity': 'one_minute_type',
        'concrete': True
    }
