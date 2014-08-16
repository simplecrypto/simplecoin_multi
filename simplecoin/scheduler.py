from decimal import Decimal, getcontext, ROUND_HALF_DOWN
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

from bitcoinrpc import CoinRPCException
from flask import current_app
from apscheduler.scheduler import Scheduler
from apscheduler.threadpool import ThreadPool
from cryptokit.base58 import get_bcaddress_version

from simplecoin import db, cache, redis_conn, create_app, currencies
from simplecoin.utils import last_block_time
from simplecoin.models import (Block, OneMinuteShare, Payout, FiveMinuteShare,
                               OneMinuteReject, OneMinuteTemperature,
                               FiveMinuteReject, OneMinuteHashrate,
                               UserSettings, FiveMinuteTemperature,
                               FiveMinuteHashrate, FiveMinuteType,
                               OneMinuteType, TradeRequest, PayoutExchange,
                               PayoutAggregate)

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
        logger.error("SQLAlchemyError occurred, rolling back: {}".format(e), exc_info=True)
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
    users = UserSettings.query.all()
    for user in users:
        user_donations.setdefault(user.user, Decimal(current_app.config.get('default_donate_perc', 0)))
        user_donations[user.user] = user.perc

    cache.set('user_donations', user_donations, timeout=1440 * 60)


@crontab
def create_aggrs():
    """
    Groups payable payouts at the end of the day by currency for easier paying
    out and database compaction, allowing deletion of regular payout records.
    """
    aggrs = {}
    adds = {}

    def get_payout_aggr(currency, user, payout_address):
        """ Create a aggregate if we don't have one for this batch,
        otherwise use the one that was already created """
        key = (currency, user, payout_address)
        if key not in aggrs:
            aggr = PayoutAggregate(currency=currency, count=0, user=user,
                                   payout_address=payout_address)
            db.session.add(aggr)
            db.session.flush()
            # silly way to defer the constraint
            aggr.amount = 0
            aggrs[key] = aggr
        return aggrs[key]

    # Attach unattached payouts in need of exchange to a new batch of
    # sellrequests

    q = Payout.query.filter_by(payable=True, aggregate_id=None).all()
    for payout in q:
        curr = currencies.lookup_address(payout.payout_address).key
        aggr = get_payout_aggr(curr, payout.user, payout.payout_address)
        payout.aggregate = aggr
        if payout.type == 1:
            aggr.amount += payout.buy_amount
        else:
            aggr.amount += payout.amount

        aggr.count += 1

    # Generate some simple stats about what we've done
    for (currency, user, payout_address), aggr in aggrs.iteritems():
        adds.setdefault(curr, [0, 0, 0])
        adds[curr][0] += aggr.amount
        adds[curr][1] += aggr.count
        adds[curr][2] += 1

    for curr, (tamount, tcount, count) in adds.iteritems():
        current_app.logger.info(
            "Created {} aggregates paying {} {} for {} payouts"
            .format(count, tamount, currency, tcount))

    if not adds:
        current_app.logger.info("No payable payouts were aggregated")

    db.session.commit()


@crontab
def create_trade_req(typ):
    """
    Takes all the payouts in need of exchanging (either buying or selling, not
    both) and attaches them to a new trade request.
    """
    reqs = {}
    adds = {}

    def get_trade_req(currency):
        """ Create a sell request if we don't have one for this batch,
        otherwise use the one that was already created """
        if currency not in reqs:
            req = TradeRequest(currency=currency, quantity=0, type=typ)
            db.session.add(req)
            db.session.flush()
            reqs[currency] = req
        return reqs[currency]

    # Attach unattached payouts in need of exchange to a new batch of
    # sellrequests

    q = PayoutExchange.query.options(db.joinedload('block'))
    # To create a sell request, we find all the payouts with no sell request
    # that are mature
    if typ == "sell":
        q = q.filter_by(sell_req=None).join(Payout.block, aliased=True)
    # To create a buy request, we find all the payouts with completed sell
    # requests that are mature
    elif typ == "buy":
        q = (q.filter_by(buy_req=None).
             join(PayoutExchange.sell_req, aliased=True).
             filter_by(_status=6).join(Payout.block, aliased=True).
             filter_by(mature=True))
    for payout in q:
        if typ == "sell":
            curr = payout.block.currency
            req = get_trade_req(curr)
            payout.sell_req = req
            # We're selling using the mined currency
            req.quantity += payout.amount
        elif typ == "buy":
            curr = currencies.lookup_address(payout.payout_address).key
            req = get_trade_req(curr)
            payout.buy_req = req
            # We're buying using the currency from the sell request
            req.quantity += payout.sell_amount

        adds.setdefault(curr, 0)
        adds[curr] += 1

    for curr, req in reqs.iteritems():
        if typ == "buy":
            current_app.logger.info("Created a buy trade request for {} with {} BTC containing {:,} PayoutExchanges"
                                    .format(req.currency, req.quantity, adds[curr]))
        else:
            current_app.logger.info("Created a sell trade request for {} {} containing {:,} PayoutExchanges"
                                    .format(req.quantity, req.currency, adds[curr]))

    if not reqs:
        current_app.logger.info("No PayoutExchange's found to create {} "
                                "requests for".format(typ))

    db.session.commit()


@crontab
def update_block_state():
    """
    Loops through all immature and non-orphaned blocks.
    First checks to see if blocks are orphaned,
    then it checks to see if they are now matured.
    """
    heights = {}

    def get_blockheight(currency):
        if currency.key not in heights:
            try:
                heights[currency.key] = currency.coinserv.getblockcount()
            except (urllib3.exceptions.HTTPError, CoinRPCException) as e:
                logger.error("Unable to communicate with {} RPC server: {}"
                             .format(currency.key, e))
                return None
        return heights[currency.key]

    # Select all immature & non-orphaned blocks
    immature = Block.query.filter_by(mature=False, orphan=False)
    for block in immature:
        try:
            currency = currencies[block.currency]
        except KeyError:
            current_app.logger.error(
                "Unable to process block {}, no currency configuration."
                .format(block))
            continue
        blockheight = get_blockheight(currency)

        # Skip checking if height difference isnt' suficcient. Avoids polling
        # the RPC server excessively
        if (blockheight - block.height) < currency.block_mature_confirms:
            logger.info("Not doing confirm check on block {} since it's not"
                        "at check threshold (last height {})"
                        .format(block, blockheight))
            continue

        try:
            # Check to see if the block hash exists in the block chain
            output = currency.coinserv.getblock(block.hash)
            logger.debug("Confirms: {}; Height diff: {}"
                         .format(output['confirmations'],
                                 blockheight - block.height))
        except urllib3.exceptions.HTTPError as e:
            logger.error("Unable to communicate with {} RPC server: {}"
                         .format(currency.key, e))
            continue
        except CoinRPCException:
            logger.info("Block {} not in coin database, assume orphan!"
                        .format(block))
            block.orphan = True
        else:
            # if the block has the proper number of confirms
            if output['confirmations'] > currency.block_mature_confirms:
                logger.info("Block {} meets {} confirms, mark mature"
                            .format(block, currency.block_mature_confirms))
                block.mature = True
                for payout in block.payouts:
                    if payout.type == 0:
                        payout.payable = True
            # else if the result shows insufficient confirms, mark orphan
            elif output['confirmations'] < currency.block_mature_confirms:
                logger.info("Block {} occured {} height ago, but not enough confirms. Marking orphan."
                            .format(block, currency.block_mature_confirms))
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
        if get_bcaddress_version(key) is not None:
            user_shares[key] = Decimal(value)
        else:
            data[key] = value

    if not user_shares:
        user_shares[data['address']] = 1

    logger.debug("Processing block with details {}".format(data))
    merged_type = data.get('merged')
    if 'start_time' in data:
        time_started = datetime.datetime.utcfromtimestamp(float(data.get('start_time')))
    else:
        time_started = last_block_time(data['algo'], merged_type=merged_type)
    total_shares = sum(user_shares.itervalues())

    block = Block.create(
        user=data['address'],
        height=data['height'],
        shares_to_solve=total_shares,
        total_value=(Decimal(data['total_subsidy']) / 100000000),
        transaction_fees=int(data['fees']),
        bits=data['hex_bits'],
        hash=data['hash'],
        time_started=time_started,
        currency=data.get('currency', merged_type),
        worker=data.get('worker'),
        found_at=datetime.datetime.utcfromtimestamp(float(data['solve_time'])),
        algo=data['algo'],
        merged_type=merged_type)

    getcontext().rounding = ROUND_HALF_DOWN

    if simulate:
        out = "\n".join(["\t".join((user,
                                    str((amount * 100) / total_shares),
                                    str(((amount * block.total_value) / total_shares).quantize(current_app.SATOSHI)),
                                    str(amount))) for user, amount in user_shares.iteritems()])
        logger.debug("Share distribution:\nUSR\t%\tBLK_PAY\tSHARE\n{}".format(out))

    logger.debug("Distribute_amnt: {}".format(block.total_value))

    # Below calculates the truncated portion going to each miner. because
    # of fractional pieces the total accrued wont equal the disitrubte_amnt
    # so we will distribute that round robin the miners in dictionary order
    accrued = 0
    user_payouts = {}
    user_extra = {}
    for user, share_count in user_shares.iteritems():
        user_earned = (share_count * block.total_value) / total_shares
        user_payouts[user] = user_earned.quantize(current_app.SATOSHI)
        user_extra[user] = user_earned - user_payouts[user]
        accrued += user_payouts[user]

    logger.debug("Total accrued after trunated iteration {}; {}%"
                 .format(accrued, (accrued / block.total_value) * 100))

    # loop over the dictionary indefinitely, paying out users with the highest
    # remainder first until we've distributed all the remaining funds
    i = 0
    while accrued < block.total_value:
        i += 1
        max_user = max(user_extra, key=user_extra.get)
        user_payouts[max_user] += current_app.SATOSHI
        accrued += current_app.SATOSHI
        user_extra.pop(max_user)
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

    default_perc = Decimal(current_app.config.get('default_donate_perc', 0))
    fee_perc = Decimal(current_app.config.get('fee_perc', 0.02))
    # convert our custom percentages that apply to these users into an
    # easy to access dictionary
    custom_percs = UserSettings.query.filter(UserSettings.user.in_(user_shares.keys()))
    custom_percs = {d.user: d.perc for d in custom_percs}

    for user, payout in user_payouts.iteritems():
        # use the custom perc, or fallback to the default
        perc = custom_percs.get(user, default_perc)
        user_perc[user] = perc

        # if the perc is greater than 0 it's calced as a donation
        if perc > 0:
            donation = (perc * payout).quantize(current_app.SATOSHI)
            logger.debug("Donation of\t{}\t({}%)\tcollected from\t{}"
                         .format(donation, perc * 100, user))
            donation_total += donation
            user_payouts[user] -= donation
            user_perc_applied[user] = donation

        # if less than zero it's a bonus payout
        elif perc < 0:
            perc *= -1
            bonus = (perc * payout).quantize(current_app.SATOSHI)
            logger.debug("Bonus of\t{}\t({}%)\tpaid to\t{}"
                         .format(bonus, perc, user))
            user_payouts[user] += bonus
            bonus_total += bonus
            user_perc_applied[user] = -1 * bonus

        # percentages of 0 are no-ops

    logger.info("Payed out {} in bonus payment"
                .format(bonus_total))
    logger.info("Received {} in donation payment"
                .format(donation_total))
    logger.info("Net income from block {}"
                .format(donation_total - bonus_total))

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
            (user, str(amount)))
            for user, amount in user_payouts.iteritems()])
        logger.debug("Final payout distribution:\nUSR\tAMNT\n{}".format(out))
        db.session.rollback()
    else:
        # record the payout for each user
        for user, amount in user_payouts.iteritems():
            if amount == 0.0:
                logger.info("Skip zero payout for USR: {}".format(user))
                continue

            # Create a payout record of the correct type. Either one indicating
            # that the currency will be exchange, or directly distributed
            if currencies.lookup_address(user).key == block.currency:
                p = Payout.create(amount=amount,
                                  block=block,
                                  user=user,
                                  shares=user_shares[user],
                                  perc=user_perc[user])
                p.payable = True
            else:
                p = PayoutExchange.create(amount=amount,
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
            shares = float(shares)
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
            sched.add_cron_job(create_trade_req, args=("sell",), second=0)
            # every minute at 55 seconds after the minute
            sched.add_cron_job(create_trade_req, args=("buy",), second=5)
            # every minute at 55 seconds after the minute
            sched.add_cron_job(collect_minutes, second=35)
            # every five minutes 20 seconds after the minute
            sched.add_cron_job(compress_minute, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=20)
            # every hour 2.5 minutes after the hour
            sched.add_cron_job(compress_five_minute, minute=2, second=30)
            # every 15 minutes 2 seconds after the minute
            sched.add_cron_job(update_block_state, second=2)
        else:
            logger.info("Stage mode has been set in the configuration, not "
                        "running scheduled database altering cron tasks")

        sched.add_cron_job(update_online_workers, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=30)
        sched.add_cron_job(cache_user_donation, minute='0,15,30,45', second=15)
        sched.add_cron_job(server_status, second=15)
        sched.start()
