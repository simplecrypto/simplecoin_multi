import logging
import datetime
import time
import urllib3
import requests
import sqlalchemy
import setproctitle
import decorator
import argparse
import json
import redis

from sqlalchemy.sql import select
from bitcoinrpc import CoinRPCException
from flask import current_app
from time import sleep
from apscheduler.scheduler import Scheduler
from apscheduler.threadpool import ThreadPool
from math import ceil, floor
from cryptokit.base58 import get_bcaddress_version

from simplecoin import db, coinservs, cache, redis_conn, create_app
from simplecoin.utils import last_block_time
from simplecoin.models import (Block, OneMinuteShare, Payout, Transaction,
                               FiveMinuteShare, OneMinuteReject,
                               OneMinuteTemperature, FiveMinuteReject,
                               OneMinuteHashrate, DonationPercent,
                               FiveMinuteTemperature, FiveMinuteHashrate,
                               FiveMinuteType, OneMinuteType)

logger = logging.getLogger('apscheduler.scheduler')


@decorator.decorator
def crontab(func, *args, **kwargs):
    """ Handles rolling back SQLAlchemy exceptions to prevent breaking the
    connection for the whole scheduler. Also records timing information into
    the cache """

    t = time.time()
    res = None
    try:
        res = func(*args, **kwargs)
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error("SQLAlchemyError occurred, rolling back: {}".format(e))
        db.session.rollback()
    except Exception:
        logger.error("Unhandled exception in {}".format(func.__name__),
                     exc_info=True)

    t = time.time() - t
    cache.cache._client.hmset('cron_last_run_{}'.format(func.__name__),
                              dict(runtime=t, time=int(time.time())))
    return res


@crontab
def cleanup():
    pass


@crontab
def update_online_workers():
    """
    Grabs a list of workers from the running powerpool instances and caches
    them
    """
    users = {}
    for i, pp_config in enumerate(current_app.config['monitor_addrs']):
        mon_addr = pp_config['mon_address'] + '/clients'
        try:
            req = requests.get(mon_addr)
            data = req.json()
        except Exception:
            logger.warn("Unable to connect to {} to gather worker summary."
                        .format(mon_addr))
        else:
            for address, workers in data['clients'].iteritems():
                users.setdefault('addr_online_' + address, [])
                for d in workers:
                    users['addr_online_' + address].append((d['worker'], i))

    cache.set_many(users, timeout=480)


@crontab
def cache_user_donation():
    """
    Grab all user donations and loop through them then cache donation %
    """
    user_donations = {}
    # Build a dict of donation % to cache
    custom_donations = DonationPercent.query.all()
    for donation in custom_donations:
        user_donations.setdefault(donation.user, current_app.config['default_perc'])
        user_donations[donation.user] = donation.perc

    cache.set('user_donations', user_donations, timeout=1440 * 60)


@crontab
def update_block_state():
    """
    Loops through all immature and non-orphaned blocks.
    First checks to see if blocks are orphaned,
    then it checks to see if they are now matured.
    """
    # Select all immature & non-orphaned blocks
    immature = Block.query.filter_by(mature=False, orphan=False)
    for block in immature:
        logger.info("Checking state of {} block height {}"
                    .format(block.merged_type or "main", block.height))
        if block.merged_type:
            merged_cfg = current_app.config['merged_cfg'][block.merged_type]
            mature_diff = merged_cfg['block_mature_confirms']
            rpc = merge_coinserv[block.merged_type]
            name = merged_cfg['name']
        else:
            mature_diff = current_app.config['block_mature_confirms']
            rpc = coinserv
            name = "main"

        try:
            blockheight = rpc.getblockcount()
        except (urllib3.exceptions.HTTPError, CoinRPCException) as e:
            logger.error("Unable to communicate with {} RPC server: {}"
                         .format(name, e))
            continue

        # ensure that our RPC server has more than caught up...
        if blockheight - 10 < block.height:
            logger.info("Skipping block {}:{} because blockchain isn't caught up."
                        "Block is height {} and blockchain is at {}"
                        .format(block.height, block.hash, block.height, blockheight))
            continue

        logger.info("Checking block height: {}".format(block.height))
        # Check to see if the block hash exists in the block chain
        try:
            output = rpc.getblock(block.hash)
            logger.debug("Confirms: {}; Height diff: {}"
                         .format(output['confirmations'],
                                 blockheight - block.height))
        except urllib3.exceptions.HTTPError as e:
            logger.error("Unable to communicate with {} RPC server: {}"
                         .format(name, e))
            continue
        except CoinRPCException:
            logger.info("Block {}:{} not in coin database, assume orphan!"
                        .format(block.height, block.hash))
            block.orphan = True
        else:
            if output['confirmations'] > mature_diff:
                logger.info("Block {}:{} meets {} confirms, mark mature"
                            .format(block.height, block.hash, mature_diff))
                block.mature = True
            elif (blockheight - block.height) > mature_diff and output['confirmations'] < mature_diff:
                logger.info("Block {}:{} {} height ago, but not enough confirms. Marking orphan."
                            .format(block.height, block.hash, mature_diff))
                block.orphan = True

        db.session.commit()


@crontab
def run_payouts(simulate=False):
    """ Loops through all the blocks that haven't been paid out and attempts
    to pay them out """
    unproc_blocks = redis_conn.keys("unproc_block*")
    for key in unproc_blocks:
        hash = key[13:]
        logger.info("Attempting to process block hash {}".format(hash))
        try:
            payout(key, simulate=simulate)
        except Exception:
            db.session.rollback()
            logger.error("Unable to payout block {}".format(hash), exc_info=True)


def payout(redis_key, simulate=False):
    """
    Calculates payouts for users from share records for the latest found block.
    """
    if simulate:
        logger.debug("Running in simulate mode, no commit will be performed")
        logger.setLevel(logging.DEBUG)

    data = {}
    user_shares = {}
    raw = redis_conn.hgetall(redis_key)
    for key, value in raw.iteritems():
        if get_bcaddress_version(key):
            user_shares[key] = float(value)
        else:
            data[key] = value

    if not user_shares:
        user_shares[data['address']] = 1

    logger.debug("Processing block with details {}".format(data))
    merged_type = data.get('merged_type')
    if 'start_time' in data:
        time_started = datetime.datetime.utcfromtimestamp(float(data.get('start_time')))
    else:
        time_started = last_block_time(data['algo'], merged_type=merged_type)
    block = Block.create(
        user=data['address'],
        height=data['height'],
        total_value=int(data['total_subsidy']),
        transaction_fees=int(data['fees']),
        bits=data['hex_bits'],
        hash=data['hash'],
        time_started=time_started,
        currency=data['currency'],
        worker=data.get('worker'),
        found_at=datetime.datetime.utcfromtimestamp(float(data['solve_time'])),
        algo=data['algo'],
        merged_type=merged_type)

    total_shares = sum(user_shares.itervalues())

    if simulate:
        out = "\n".join(["\t".join((user,
                                    str((amount * 100.0) / total_shares),
                                    str((amount * block.total_value) // total_shares),
                                    str(amount))) for user, amount in user_shares.iteritems()])
        logger.debug("Share distribution:\nUSR\t%\tBLK_PAY\tSHARE\n{}".format(out))

    logger.debug("Distribute_amnt: {}".format(block.total_value))

    # Below calculates the truncated portion going to each miner. because
    # of fractional pieces the total accrued wont equal the disitrubte_amnt
    # so we will distribute that round robin the miners in dictionary order
    accrued = 0
    user_payouts = {}
    for user, share_count in user_shares.iteritems():
        user_payouts[user] = float(share_count * block.total_value) // total_shares
        accrued += user_payouts[user]

    logger.debug("Total accrued after trunated iteration {}; {}%"
                 .format(accrued, (accrued / float(block.total_value)) * 100))
    # loop over the dictionary indefinitely until we've distributed
    # all the remaining funds
    i = 0
    while accrued < block.total_value:
        for key in user_payouts:
            i += 1
            user_payouts[key] += 1
            accrued += 1
            # exit if we've exhausted
            if accrued >= block.total_value:
                break

    logger.debug("Ran round robin distro {} times to finish distrib".format(i))

    # now handle donation or bonus distribution for each user
    donation_total = 0
    bonus_total = 0
    # dictionary keyed by address to hold donate/bonus percs and amnts
    user_perc_applied = {}
    user_perc = {}

    default_perc = current_app.config.get('default_perc', 0)
    # convert our custom percentages that apply to these users into an
    # easy to access dictionary
    custom_percs = DonationPercent.query.filter(DonationPercent.user.in_(user_shares.keys()))
    custom_percs = {d.user: d.perc for d in custom_percs}

    for user, payout in user_payouts.iteritems():
        # use the custom perc, or fallback to the default
        perc = custom_percs.get(user, default_perc)
        user_perc[user] = perc

        # if the perc is greater than 0 it's calced as a donation
        if perc > 0:
            donation = int(ceil((perc / 100.0) * payout))
            logger.debug("Donation of\t{}\t({}%)\tcollected from\t{}"
                         .format(donation / 100000000.0, perc, user))
            donation_total += donation
            user_payouts[user] -= donation
            user_perc_applied[user] = donation

        # if less than zero it's a bonus payout
        elif perc < 0:
            perc *= -1
            bonus = int(floor((perc / 100.0) * payout))
            logger.debug("Bonus of\t{}\t({}%)\tpaid to\t{}"
                         .format(bonus / 100000000.0, perc, user))
            user_payouts[user] += bonus
            bonus_total += bonus
            user_perc_applied[user] = -1 * bonus

        # percentages of 0 are no-ops

    logger.info("Payed out {} in bonus payment"
                .format(bonus_total / 100000000.0))
    logger.info("Received {} in donation payment"
                .format(donation_total / 100000000.0))
    logger.info("Net income from block {}"
                .format((donation_total - bonus_total) / 100000000.0))

    assert accrued == block.total_value
    logger.info("Successfully distributed all rewards among {} users."
                .format(len(user_payouts)))

    # run another safety check
    user_sum = sum(user_payouts.values())
    assert user_sum == (block.total_value + bonus_total - donation_total)
    logger.info("Double check for payout distribution."
                " Total user payouts {}, total block value {}."
                .format(user_sum, block.total_value))

    if simulate:
        out = "\n".join(["\t".join(
            (user, str(amount / 100000000.0)))
            for user, amount in user_payouts.iteritems()])
        logger.debug("Final payout distribution:\nUSR\tAMNT\n{}".format(out))
        db.session.rollback()
    else:
        # record the payout for each user
        for user, amount in user_payouts.iteritems():
            if amount == 0.0:
                logger.info("Skip zero payout for USR: {}".format(user))
                continue
            Payout.create(amount=amount,
                          block=block,
                          user=user,
                          shares=user_shares[user],
                          perc=user_perc[user])

        # update the block status and collected amounts
        block.donated = donation_total
        block.bonus_payed = bonus_total
        db.session.commit()
        redis_conn.delete(redis_key)


@crontab
def collect_minutes():
    """ Grabs all the pending minute shares out of redis and puts them in the
    database """
    def count_share(typ, minute, amount, user, worker=''):
        logger.debug("Adding {} for {} of amount {}"
                     .format(typ.__name__, user, amount))
        try:
            typ.create(user, amount, minute, worker)
            db.session.commit()
        except sqlalchemy.exc.IntegrityError:
            db.session.rollback()
            typ.add_value(user, amount, minute, worker)
            db.session.commit()

    unproc_mins = redis_conn.keys("min_*")
    for key in unproc_mins:
        current_app.logger.info("Processing key {}".format(key))
        share_type, algo, stamp = key.split("_")[1:]
        minute = datetime.datetime.utcfromtimestamp(float(stamp))
        if stamp < (time.time() - 30):
            current_app.logger.info("Skipping timestamp {}, too young".format(minute))
            continue
        redis_conn.rename(key, "processing_shares")
        for user, shares in redis_conn.hgetall("processing_shares").iteritems():
            # messily parse out the worker/address combo...
            parts = user.split(".")
            if len(parts) > 0:
                worker = parts[1]
            else:
                worker = ''
            address = parts[0]

            # we want to log how much of each type of reject for the whole pool
            if address == "pool":
                if share_type == "acc":
                    count_share(OneMinuteShare, minute, shares, "pool_{}".format(algo))
                if share_type == "low":
                    count_share(OneMinuteReject, minute, shares, "pool_low_diff_{}".format(algo))
                if share_type == "dup":
                    count_share(OneMinuteReject, minute, shares, "pool_dup_{}".format(algo))
                if share_type == "stale":
                    count_share(OneMinuteReject, minute, shares, "pool_stale_{}".format(algo))
            # only log a total reject on a per-user basis
            else:
                if share_type == "acc":
                    count_share(OneMinuteShare, minute, shares, "{}_{}".format(address, algo))
                if share_type == "stale":
                    count_share(OneMinuteReject, minute, shares, "{}_{}".format(address, algo))
        redis_conn.delete("processing_shares")


@crontab
def compress_minute():
    """ Compresses OneMinute records (for temp, hashrate, shares, rejects) to
    FiveMinute """
    OneMinuteShare.compress()
    OneMinuteReject.compress()
    OneMinuteTemperature.compress()
    OneMinuteHashrate.compress()
    OneMinuteType.compress()
    db.session.commit()


@crontab
def compress_five_minute():
    FiveMinuteShare.compress()
    FiveMinuteReject.compress()
    FiveMinuteTemperature.compress()
    FiveMinuteHashrate.compress()
    FiveMinuteType.compress()
    db.session.commit()


@crontab
def server_status():
    """
    Periodicly poll the backend to get number of workers and throw it in the
    cache
    """
    total_workers = 0
    servers = []
    raw_servers = {}
    for i, pp_config in enumerate(current_app.config['monitor_addrs']):
        mon_addr = pp_config['mon_address']
        try:
            req = requests.get(mon_addr)
            data = req.json()
        except Exception:
            logger.warn("Couldn't connect to internal monitor at {}"
                        .format(mon_addr))
            continue
        else:
            if 'server' in data:
                workers = data['stratum_manager']['client_count_authed']
                hashrate = data['stratum_manager']['mhps'] * 1000000
                raw_servers[pp_config['stratum']] = data
            else:
                workers = data['stratum_clients']
                hashrate = data['shares']['hour_total'] / 3600.0 * current_app.config['hashes_per_share']
            servers.append(dict(workers=workers,
                                hashrate=hashrate,
                                name=pp_config['stratum']))
            total_workers += workers

    cache.set('raw_server_status', json.dumps(raw_servers), timeout=1200)
    cache.set('server_status', json.dumps(servers), timeout=1200)
    cache.set('total_workers', total_workers, timeout=1200)


app = create_app(celery=True)


# monkey patch the thread pool for flask contexts
ThreadPool._old_run_jobs = ThreadPool._run_jobs
def _run_jobs(self, core):
    logger.debug("Starting patched threadpool worker!")
    with app.app_context():
        ThreadPool._old_run_jobs(self, core)
ThreadPool._run_jobs = _run_jobs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='simplecoin task scheduler')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    args = parser.parse_args()

    with app.app_context():
        root = logging.getLogger()
        hdlr = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
        hdlr.setFormatter(formatter)
        root.addHandler(hdlr)
        root.setLevel(getattr(logging, args.log_level))

        sched = Scheduler(standalone=True)
        logger.info("=" * 80)
        logger.info("SimpleCoin cron scheduler starting up...")
        setproctitle.setproctitle("simplecoin_scheduler")

        # All these tasks actually change the database, and shouldn't
        # be run by the staging server
        if not current_app.config.get('stage', False):
            # every minute at 55 seconds after the minute
            sched.add_cron_job(run_payouts, second=55)
            # every minute at 55 seconds after the minute
            sched.add_cron_job(collect_minutes, second=35)
            # every five minutes 20 seconds after the minute
            sched.add_cron_job(compress_minute, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=20)
            # every hour 2.5 minutes after the hour
            sched.add_cron_job(compress_five_minute, minute=2, second=30)
            # every 15 minutes 2 seconds after the minute
            sched.add_cron_job(update_block_state, minute='0,15,30,45', second=2)
        else:
            logger.info("Stage mode has been set in the configuration, not "
                        "running scheduled database altering cron tasks")

        sched.add_cron_job(update_online_workers, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=30)
        sched.add_cron_job(cache_user_donation, minute='0,15,30,45', second=15)
        sched.add_cron_job(server_status, second=15)
        sched.start()
