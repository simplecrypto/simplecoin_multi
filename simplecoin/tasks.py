from math import ceil, floor
import json
import logging
import datetime
import time
import urllib3

from flask import current_app
from sqlalchemy.sql import func
from time import sleep
import sqlalchemy

from celery import Celery
from simplecoin import db, coinserv, cache, merge_coinserv
from simplecoin.utils import last_block_share_id_nocache, last_block_time_nocache, last_block_time, get_round_shares, \
    last_block_share_id
from simplecoin.models import (
    Share, Block, OneMinuteShare, Payout, Transaction, Blob, FiveMinuteShare,
    Status, OneMinuteReject, OneMinuteTemperature, FiveMinuteReject,
    OneMinuteHashrate, Threshold, Event, DonationPercent, BonusPayout,
    FiveMinuteTemperature, FiveMinuteHashrate, FiveMinuteType, OneMinuteType,
    MergeAddress)
from sqlalchemy.sql import select
from cryptokit import bits_to_shares, bits_to_difficulty

from bitcoinrpc import CoinRPCException
from celery.utils.log import get_task_logger
import requests


logger = get_task_logger(__name__)
celery = Celery('simplecoin')


@celery.task(bind=True)
def add_share(self, user, shares):
    """
    Adds a round share to postgresql

    user: should be a username/wallet address
    shares: should be an integer representation of n1 shares
    """
    try:
        share_mult = current_app.config.get('share_multiplier', 1)
        Share.create(user=user, shares=shares * share_mult)
        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in add share", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def add_block(self, user, height, total_value, transaction_fees, bits,
              hash_hex, merged=None, worker=None, **kwargs):
    """
    Insert a discovered block & blockchain data

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
    if merged is True:
        merged = 'MON'

    logger.warn(
        "Received an add block notification!\nUser: {}\nHeight: {}\n"
        "Total Height: {}\nTransaction Fees: {}\nBits: {}\nHash Hex: {}"
        .format(user, height, total_value, transaction_fees, bits, hash_hex))
    try:
        last = last_block_share_id_nocache(merged)
        block = Block.create(user,
                             height,
                             total_value,
                             transaction_fees,
                             bits,
                             hash_hex,
                             time_started=last_block_time_nocache(merged),
                             merged_type=merged,
                             worker=worker)
        try:
            db.session.flush()
        except sqlalchemy.exc.IntegrityError:
            logger.warn("A duplicate block notification was received, ignoring...!")
            db.session.rollback()
            return
        count = (db.session.query(func.sum(Share.shares)).
                 filter(Share.id > last).
                 filter(Share.id <= block.last_share_id).scalar()) or 128
        block.shares_to_solve = count
        db.session.commit()

        # Expire cache values for round
        cache.delete_memoized(last_block_share_id)
        cache.delete_memoized(last_block_time)
        cache.delete_memoized(get_round_shares)

    except Exception as exc:
        logger.error("Unhandled exception in add_block", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def add_one_minute(self, user, valid_shares, minute, worker='', dup_shares=0,
                   low_diff_shares=0, stale_shares=0):
    """
    Adds a new single minute entry for a user

    minute: timestamp (int)
    shares: number of shares received over the timespan
    user: string of the user
    """
    share_mult = current_app.config.get('share_multiplier', 1)
    def count_share(typ, amount, user_=user):
        logger.debug("Adding {} for {} of amount {}"
                     .format(typ.__name__, user_, amount))
        try:
            typ.create(user_, amount * share_mult, minute, worker)
            db.session.commit()
        except sqlalchemy.exc.IntegrityError:
            db.session.rollback()
            typ.add_value(user_, amount * share_mult, minute, worker)
            db.session.commit()

    try:
        # log their valid shares
        if valid_shares:
            count_share(OneMinuteShare, valid_shares)

        # we want to log how much of each type of reject for the whole pool
        if user == "pool":
            if low_diff_shares:
                count_share(OneMinuteReject, low_diff_shares, user_="pool_low_diff")
            if dup_shares:
                count_share(OneMinuteReject, dup_shares, user_="pool_dup")
            if stale_shares:
                count_share(OneMinuteReject, stale_shares, user_="pool_stale")

        # only log a total reject on a per-user basis
        else:
            total_reject = stale_shares
            if total_reject:
                count_share(OneMinuteReject, total_reject)
    except Exception as exc:
        logger.error("Unhandled exception in add_one_minute", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def new_block(self, blockheight, bits=None, reward=None):
    """
    Notification that a new block height has been reached in the network.
    Sets some things into the cache for display on the website, adds graphing
    for the network difficulty graph.

    DEPRECATED
    """
    logger.warn("New Block call is now deprecated, please set send_new_block to False in powerpool")


@celery.task(bind=True)
def agent_receive(self, address, worker, typ, payload, timestamp):
    """ Accepts ppagent data that is forwarded from powerpool and manages
    adding it to the database and triggering alerts as needed. """
    # convert unix timestamp to datetime
    dt = datetime.datetime.utcfromtimestamp(timestamp)

    def inject_device_stat(cls, device, value):
        if value:
            stat = cls(user=address, worker=worker, device=device, value=value,
                       time=dt)
            db.session.merge(stat)

    try:
        # if they passed a threshold we should update the database object
        if typ == "thresholds":
            try:
                if not payload:
                    # if they didn't list valid email key we want to remove
                    Threshold.query.filter_by(worker=worker, user=address).delete()
                else:
                    thresh = Threshold(
                        worker=worker,
                        user=address,
                        green_notif=not payload.get('no_green_notif', False),
                        temp_thresh=payload.get('overheat'),
                        hashrate_thresh=payload.get('lowhashrate'),
                        offline_thresh=payload.get('offline'),
                        emails=payload['emails'][:4])
                    db.session.merge(thresh)
            except KeyError:
                # assume they're trying to remove the thresholds...
                Threshold.query.filter_by(worker=worker, user=address).delete()
                logger.warn("Bad payload was sent as Threshold data: {}"
                            .format(payload))
            db.session.commit()
            return
        elif typ == 'status':
            ret = (db.session.query(Status).filter_by(user=address, worker=worker).
                   update({"status": json.dumps(payload), "time": dt}))
            # if the update affected nothing
            if ret == 0:
                new = Status(user=address, worker=worker,
                             status=json.dumps(payload), time=dt)
                db.session.add(new)
            db.session.commit()
            return

        # the two status messages can trigger a threshold condition, so we need
        # to load the threshold to check
        thresh = Threshold.query.filter_by(worker=worker, user=address).first()
        if typ == 'temp':
            # track the overheated cards
            overheat_cards = []
            temps = []
            for i, value in enumerate(payload):
                inject_device_stat(OneMinuteTemperature, i, value)
                # report over temperature
                if thresh and value >= thresh.temp_thresh:
                    overheat_cards.append(str(i))
                    temps.append(str(value))

            if overheat_cards and not thresh.temp_err:
                s = "s" if len(overheat_cards) else ""
                thresh.report_condition(
                    "Worker {}, overheat on card{s} {}, temp{s} {}"
                    .format(worker, ', '.join(overheat_cards), ', '.join(temps),
                            s=s),
                    'temp_err', True)
            elif not overheat_cards and thresh and thresh.temp_err:
                thresh.report_condition(
                    "Worker {} overheat condition relieved".format(worker),
                    'temp_err', False)

        elif typ == 'hashrate':
            for i, value in enumerate(payload):
                # multiply by a million to turn megahashes to hashes
                inject_device_stat(OneMinuteHashrate, i, value * 1000000)

            # do threshold checking if they have one set
            if thresh:
                hr = sum(payload) * 1000
                if int(hr) == 0:
                    logger.warn("Entry with 0 hashrate. Worker {}; User {}".format(worker, address))
                else:
                    low_hash = thresh and hr <= thresh.hashrate_thresh
                    if low_hash and not thresh.hashrate_err:
                        thresh.report_condition(
                            "Worker {} low hashrate condition, hashrate {} KH/s"
                            .format(worker, hr), 'hashrate_err', True)
                    elif not low_hash and thresh.hashrate_err:
                        thresh.report_condition(
                            "Worker {} low hashrate condition resolved, hashrate {} KH/s"
                            .format(worker, hr), 'hashrate_err', False)
        else:
            logger.warning("Powerpool sent an unkown agent message of type {}"
                           .format(typ))
        try:
            db.session.commit()
        except sqlalchemy.exc.IntegrityError:
            db.session.rollback()
            logger.warn("Received a duplicate agent msg of typ {}, ignoring...!"
                        .format(typ))
    except Exception:
        logger.error("Unhandled exception in update_status", exc_info=True)
        db.session.rollback()
