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

from sqlalchemy.sql import select
from bitcoinrpc import CoinRPCException
from flask import current_app
from time import sleep
from apscheduler.scheduler import Scheduler
from apscheduler.threadpool import ThreadPool
from math import ceil, floor
from cryptokit.base58 import get_bcaddress_version

from simplecoin import create_app
from simplecoin import db, coinserv, cache, merge_coinserv, redis
from simplecoin.models import (
    Share, Block, OneMinuteShare, Payout, Transaction, Blob, FiveMinuteShare,
    Status, OneMinuteReject, OneMinuteTemperature, FiveMinuteReject,
    OneMinuteHashrate, Threshold, Event, DonationPercent, BonusPayout,
    FiveMinuteTemperature, FiveMinuteHashrate, FiveMinuteType, OneMinuteType,
    MergeAddress)

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


def get_sharemap(start_id, shares_to_fetch, chunk_size=None, sleep_interval=None):
    """ Give a share id to start at and a number of shares to fetch (round size),
    returns a map of {user_address: share_count} format and how many shares
    were actually accrued """
    # allow overridable configuration defaults
    chunk_size = chunk_size or current_app.config.get('sharemap_chunk_size', 10000)
    sleep_interval = sleep_interval or current_app.config.get('sharemap_sleep_interval', 0.1)
    # hold all user shares here
    user_shares = {}
    stop_id = Share.query.order_by(Share.id).first().id  # last share to look for
    if start_id is None:
        # start at beginning
        start_id = Share.query.order_by(Share.id.desc()).first().id
    # iterate through shares in newest to oldest order to find the share
    # id that is oldest needed id
    remain = shares_to_fetch
    rows = 0
    sleep_total = 0
    start_time = time.time()
    while remain > 0 and start_id > stop_id:
        res = (db.engine.execution_options(stream_results=True).
               execute(select([Share.shares, Share.user]).
                       order_by(Share.id.desc()).
                       where(Share.id > start_id - chunk_size).
                       where(Share.id <= start_id)))
        chunk = res.fetchall()
        logger.debug("Fetching rows {:,} to {:,}".format(start_id - chunk_size, start_id))
        for shares, user in chunk:
            rows += 1
            user_shares.setdefault(user, 0)
            if remain > shares:
                user_shares[user] += shares
                remain -= shares
            else:
                user_shares[user] += remain
                remain = 0
                break
        else:
            # grab another batch, we've still got shares to find
            sleep(sleep_interval)
            sleep_total += sleep_interval
            start_id -= 10000
            continue

        # if we broke from the for loop we're done, exit
        break

    logger.info("Slept for a total of {}"
                .format(datetime.timedelta(seconds=sleep_total)))
    logger.info("Queried and summed for a total of {}"
                .format(datetime.timedelta(seconds=time.time() - start_time - sleep_total)))
    logger.info("Iterated {:,} rows to find {:,} shares"
                .format(rows, shares_to_fetch))
    logger.info("Found {:,} unique users in share log".format(len(user_shares)))

    return user_shares, shares_to_fetch - remain


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
    unproc_blocks = redis.keys("unproc_block*")
    for key in unproc_blocks:
        hash = key[12:]
        logger.info("Attempting to process block hash {}".format(hash))
        payout(key, simulate=simulate)


def payout(key, simulate=False):
    """
    Calculates payouts for users from share records for the latest found block.
    """
    if simulate:
        logger.debug("Running in simulate mode, no commit will be performed")
        logger.setLevel(logging.DEBUG)

    data = {}
    user_shares = {}
    raw = redis.hgetall(key)
    for key, value in raw.iteritems():
        if get_bcaddress_version(key):
            user_shares[key] = value
        else:
            data[key] = value

    logger.debug("Processing block with details {}".format(data))
    total_shares = sum(user_shares.itervalues())

    if simulate:
        out = "\n".join(["\t".join((user,
                                    str((amount * 100.0) / total_shares),
                                    str((amount * data['total_value']) // total_shares),
                                    str(amount))) for user, amount in user_shares.iteritems()])
        logger.debug("Share distribution:\nUSR\t%\tBLK_PAY\tSHARE\n{}".format(out))

    logger.debug("Distribute_amnt: {}".format(data['total_value']))

    # Below calculates the truncated portion going to each miner. because
    # of fractional pieces the total accrued wont equal the disitrubte_amnt
    # so we will distribute that round robin the miners in dictionary order
    accrued = 0
    user_payouts = {}
    for user, share_count in user_shares.iteritems():
        user_payouts[user] = float(share_count * data['total_value']) // total_shares
        accrued += user_payouts[user]

    logger.debug("Total accrued after trunated iteration {}; {}%"
                 .format(accrued, (accrued / float(data['total_value'])) * 100))
    # loop over the dictionary indefinitely until we've distributed
    # all the remaining funds
    i = 0
    while accrued < data['total_value']:
        for key in user_payouts:
            i += 1
            user_payouts[key] += 1
            accrued += 1
            # exit if we've exhausted
            if accrued >= data['total_value']:
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

    assert accrued == data['total_value']
    logger.info("Successfully distributed all rewards among {} users."
                .format(len(user_payouts)))

    # run another safety check
    user_sum = sum(user_payouts.values())
    assert user_sum == (data['total_value'] + bonus_total - donation_total)
    logger.info("Double check for payout distribution."
                " Total user payouts {}, total block value {}."
                .format(user_sum, data['total_value']))

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
                          block=hash,
                          shares_contributed=user_shares[user],
                          perc=user_perc[user])

        block = Block.create(
            user=data['address'],
            total_value=data['total_subsidy'],
            transaction_fees=data['fees'],
            bits=data['bits'],
            found_at=datetime.datetime.utcfromtimestamp(data['solve_time']),
            currency=data['currency'],
            hash=data['hash'],
            height=data['height'],
            time_started=datetime.datetime.utcfromtimestamp(data.get('start_time')) or )

        # update the block status and collected amounts
        block.donated = donation_total
        block.bonus_payed = bonus_total


@crontab
def collect_minutes():
    """ Grabs all the pending minute shares out of redis and puts them in the
    database """
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

    last = ((time.time() // 60) * 60) - 60
    for stamp in xrange(last, last - 600, -60):
        for algo in current_app.config['algos']:
            for share_type in ["acc", "dup", "low", "stale"]:
                key = "{}-min-{}-{}".format(share_type, algo, stamp)
                try:
                    redis.rename(key, "processing_shares")
                except redis.ResponseError:
                    pass
                else:
                    for user, shares in redis.hgetall("processing_shares").iteritems():


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
