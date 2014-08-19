import logging
import datetime
import time
import urllib3
import sqlalchemy
import decorator
import argparse
import json

from decimal import Decimal, getcontext, ROUND_HALF_DOWN
from bitcoinrpc import CoinRPCException
from flask import current_app
from flask.ext.script import Manager
from apscheduler.threadpool import ThreadPool
from cryptokit.base58 import address_version

from simplecoin import db, cache, redis_conn, create_app, currencies, powerpools
from simplecoin.utils import last_block_time, RemoteException
from simplecoin.models import (Block, Payout, UserSettings, TradeRequest,
                               PayoutExchange, PayoutAggregate, ShareSlice)

SchedulerCommand = Manager(usage='Run timed tasks manually')


@SchedulerCommand.command
def reload_cached():
    """ Recomputes all the cached values that normally get refreshed by tasks.
    Good to run if celery has been down, site just setup, etc. """
    update_online_workers()
    cache_user_donation()
    server_status()


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
        current_app.logger.error("SQLAlchemyError occurred, rolling back: {}".format(e), exc_info=True)
        db.session.rollback()
    except Exception:
        current_app.logger.error("Unhandled exception in {}".format(func.__name__),
                                exc_info=True)

    t = time.time() - t
    cache.cache._client.hmset('cron_last_run_{}'.format(func.__name__),
                              dict(runtime=t, time=int(time.time())))
    return res


@crontab
@SchedulerCommand.command
def cleanup():
    pass


@crontab
@SchedulerCommand.command
def update_online_workers():
    """
    Grabs data on all currently connected clients. Forms a dictionary of this form:
        dict(address=dict(worker_name=dict(powerpool_id=connection_count)))
    And caches each addresses connection summary as a single cache key.
    """
    users = {}
    for ppid, powerpool in powerpools.iteritems():
        try:
            data = powerpool.request('clients')
        except RemoteException:
            current_app.logger.warn("Unable to connect to {} to gather worker summary."
                                    .format(powerpool))
            continue

        for address, connections in data['clients'].iteritems():
            user = users.setdefault('addr_online_' + address, {})
            for connection in connections:
                worker = user.setdefault(connection['worker'], {})
                worker.setdefault(ppid, 0)
                user[ppid] += 1

    cache.set_many(users, timeout=480)


@crontab
@SchedulerCommand.command
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
@SchedulerCommand.command
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
@SchedulerCommand.command
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
@SchedulerCommand.command
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
                current_app.logger.error("Unable to communicate with {} RPC server: {}"
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
            current_app.logger.info("Not doing confirm check on block {} since it's not"
                        "at check threshold (last height {})"
                        .format(block, blockheight))
            continue

        try:
            # Check to see if the block hash exists in the block chain
            output = currency.coinserv.getblock(block.hash)
            current_app.logger.debug("Confirms: {}; Height diff: {}"
                         .format(output['confirmations'],
                                 blockheight - block.height))
        except urllib3.exceptions.HTTPError as e:
            current_app.logger.error("Unable to communicate with {} RPC server: {}"
                         .format(currency.key, e))
            continue
        except CoinRPCException:
            current_app.logger.info("Block {} not in coin database, assume orphan!"
                        .format(block))
            block.orphan = True
        else:
            # if the block has the proper number of confirms
            if output['confirmations'] > currency.block_mature_confirms:
                current_app.logger.info("Block {} meets {} confirms, mark mature"
                            .format(block, currency.block_mature_confirms))
                block.mature = True
                for payout in block.payouts:
                    if payout.type == 0:
                        payout.payable = True
            # else if the result shows insufficient confirms, mark orphan
            elif output['confirmations'] < currency.block_mature_confirms:
                current_app.logger.info("Block {} occured {} height ago, but not enough confirms. Marking orphan."
                            .format(block, currency.block_mature_confirms))
                block.orphan = True

        db.session.commit()


@crontab
@SchedulerCommand.option('-ds', '--dont-simulate', default=False, action="store_true")
def run_payouts(dont_simulate=False):
    """ Loops through all the blocks that haven't been paid out and attempts
    to pay them out """
    simulate = not dont_simulate
    unproc_blocks = redis_conn.keys("unproc_block*")
    for key in unproc_blocks:
        hash = key[13:]
        current_app.logger.info("Attempting to process block hash {}".format(hash))
        try:
            payout(key, simulate=simulate)
        except Exception:
            db.session.rollback()
            current_app.logger.error("Unable to payout block {}".format(hash), exc_info=True)


def payout(redis_key, simulate=False):
    """
    Calculates payouts for users from share records for the latest found block.
    """
    if simulate:
        current_app.logger.debug("Running in simulate mode, no commit will be performed")
        current_app.logger.setLevel(logging.DEBUG)

    data = {}
    user_shares = {}
    raw = redis_conn.hgetall(redis_key)
    for key, value in raw.iteritems():
        try:
            address_version(key)
        except AttributeError:
            data[key] = value
        else:
            user_shares[key] = Decimal(value)

    if not user_shares:
        user_shares[data['address']] = 1

    current_app.logger.debug("Processing block with details {}".format(data))
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
        current_app.logger.debug("Share distribution:\nUSR\t%\tBLK_PAY\tSHARE\n{}".format(out))

    current_app.logger.debug("Distribute_amnt: {}".format(block.total_value))

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

    current_app.logger.debug("Total accrued after trunated iteration {}; {}%"
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

    current_app.logger.debug("Ran round robin distro {} times to finish distrib".format(i))

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
            current_app.logger.debug("Donation of\t{}\t({}%)\tcollected from\t{}"
                                     .format(donation, perc * 100, user))
            donation_total += donation
            user_payouts[user] -= donation
            user_perc_applied[user] = donation

        # if less than zero it's a bonus payout
        elif perc < 0:
            perc *= -1
            bonus = (perc * payout).quantize(current_app.SATOSHI)
            current_app.logger.debug("Bonus of\t{}\t({}%)\tpaid to\t{}"
                                     .format(bonus, perc, user))
            user_payouts[user] += bonus
            bonus_total += bonus
            user_perc_applied[user] = -1 * bonus

        # percentages of 0 are no-ops

    current_app.logger.info("Payed out {} in bonus payment"
                            .format(bonus_total))
    current_app.logger.info("Received {} in donation payment"
                            .format(donation_total))
    current_app.logger.info("Net income from block {}"
                            .format(donation_total - bonus_total))

    assert accrued == block.total_value
    current_app.logger.info("Successfully distributed all rewards among {} users."
                            .format(len(user_payouts)))

    # run another safety check
    user_sum = sum(user_payouts.values())
    assert user_sum == (block.total_value + bonus_total - donation_total)
    current_app.logger.info("Double check for payout distribution."
                            " Total user payouts {}, total block value {}."
                            .format(user_sum, block.total_value))

    if simulate:
        out = "\n".join(["\t".join(
            (user, str(amount))) for user, amount in user_payouts.iteritems()])
        current_app.logger.debug("Final payout distribution:\nUSR\tAMNT\n{}".format(out))
        db.session.rollback()
    else:
        # record the payout for each user
        for user, amount in user_payouts.iteritems():
            if amount == 0.0:
                current_app.logger.info("Skip zero payout for USR: {}".format(user))
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
@SchedulerCommand.command
def collect_minutes():
    """ Grabs all the pending minute shares out of redis and puts them in the
    database """
    unproc_mins = redis_conn.keys("min_*")
    for key in unproc_mins:
        current_app.logger.info("Processing key {}".format(key))
        share_type, algo, stamp = key.split("_")[1:]
        minute = datetime.datetime.utcfromtimestamp(float(stamp))
        # To ensure invalid stampt don't get committed
        minute = ShareSlice.floor_time(minute, 0)
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

            try:
                slc = ShareSlice(user=address, time=minute, worker=worker, algo=algo,
                                 share_type=share_type, value=shares, span=0)
                db.session.add(slc)
                db.session.commit()
            except sqlalchemy.exc.IntegrityError:
                db.session.rollback()
                slc = ShareSlice.query.with_lockmode('update').filter_by(
                    user=address, time=minute, worker=worker, algo=algo,
                    share_type=share_type).one()
                slc.value += shares
                db.session.commit()
        redis_conn.delete("processing_shares")


@crontab
@SchedulerCommand.command
def compress_minute():
    ShareSlice.compress(0)
    db.session.commit()


@crontab
@SchedulerCommand.command
def compress_five_minute():
    ShareSlice.compress(1)
    db.session.commit()


@crontab
@SchedulerCommand.command
def server_status():
    """
    Periodicly poll the backend to get number of workers and other general
    status information.
    """
    total_workers = 0
    servers = []
    raw_servers = {}
    for powerpool in powerpools.itervalues():
        try:
            data = powerpool.request('')
        except Exception:
            current_app.logger.warn("Couldn't connect to internal monitor at {}"
                                    .format(powerpool))
            continue
        else:
            # If the server is version 0.5
            if 'server' in data:
                workers = data['stratum_manager']['client_count_authed']
                hashrate = data['stratum_manager']['mhps'] * 1000000
                raw_servers[powerpool.stratum_address] = data
            # If the server is version 0.4 or earlier. DEPRECATED
            else:
                workers = data['stratum_clients']
                hashrate = data['shares']['hour_total'] / 3600.0 * current_app.config['hashes_per_share']
            servers.append(dict(workers=workers,
                                hashrate=hashrate,
                                name=powerpool.stratum_address))
            total_workers += workers

    cache.set('raw_server_status', json.dumps(raw_servers), timeout=1200)
    cache.set('server_status', json.dumps(servers), timeout=1200)
    cache.set('total_workers', total_workers, timeout=1200)


# monkey patch the thread pool for flask contexts
def _run_jobs(self, core):
    current_app.logger.debug("Starting patched threadpool worker!")
    with self.app.app_context():
        ThreadPool._old_run_jobs(self, core)
ThreadPool._run_jobs = _run_jobs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='simplecoin task scheduler')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    args = parser.parse_args()

    app = create_app("scheduler", log_level=args.log_level)
