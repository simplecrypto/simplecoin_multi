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

    data = redis_conn.hgetall(redis_key)

    current_app.logger.debug("Processing block with details {}".format(data))
    merged_type = data.get('merged')
    if 'start_time' in data:
        time_started = datetime.datetime.utcfromtimestamp(float(data.get('start_time')))
    else:
        time_started = last_block_time(data['algo'], merged_type=merged_type)

    block = Block.create(
        user=data['address'],
        height=data['height'],
        shares_to_solve=0,
        total_value=(Decimal(data['total_subsidy']) / 100000000),
        transaction_fees=(Decimal(data['fees']) / 100000000),
        bits=data['hex_bits'],
        hash=data['hash'],
        time_started=time_started,
        currency=data.get('currency', merged_type),
        worker=data.get('worker'),
        found_at=datetime.datetime.utcfromtimestamp(float(data['solve_time'])),
        algo=data['algo'],
        merged_type=merged_type)

    # We want to determine each user's shares based on the payout method
    # of that share chain. Those shares will then be paid out proportionally
    # with payout_chain()
    for chain_id in data['chains']:
        chain_config = current_app.powerpools[chain_id]
        share_compute_functions = {'PPLNS': pplns_share_calc,
                                   'PROP': prop_share_calc}

        user_shares = share_compute_functions[chain_config['payout_type']]
        if not user_shares:
            user_shares[block.user] = 1
        payout_chain(block, user_shares, chain_id, simulate=simulate)

    db.session.commit()
    redis_conn.delete(redis_key)


def payout_chain(block, user_shares, sharechain_id, simulate=False):

    # Calculate each user's share
    # =======================================================================
    # Grab total shares to pay out
    total_shares = sum(user_shares.itervalues())
    # Set python Decimal rounding semantic
    getcontext().rounding = ROUND_HALF_DOWN

    if simulate:
        out = "\n".join(
            ["\t".join((user, str((amount * 100) / total_shares),
                        str(((amount * block.total_value) / total_shares).
                            quantize(current_app.SATOSHI)),
                        str(amount))) for user, amount in user_shares.iteritems()])
        current_app.logger.debug("Share distribution:\nUSR\t%\tBLK_PAY\tSHARE"
                                 "\n{}".format(out))

    current_app.logger.debug("Distribute_amnt: {}".format(block.total_value))
    current_app.logger.debug("Share Value: {}".format(block.total_value/total_shares))

    # Below calculates the portion going to each miner. Note that the amount
    # is not rounded or truncated - this amount is actually not payable as-is
    user_payouts = {}
    for user, share_count in user_shares.iteritems():
        user_payouts[user] = (share_count * block.total_value) / total_shares

    # Check this section
    assert sum(user_payouts.itervalues()) == block.total_value
    current_app.logger.info("Successfully allocated all rewards among {} "
                            "users.".format(len(user_payouts)))

    # Adjust each user's payout to include donations, fees, and bonuses
    # =======================================================================
    # Grab all customized user settings out of the DB
    custom_settings = UserSettings.query.\
        filter(UserSettings.user.in_(user_shares.keys())).all()
    # Grab defaults percs from config
    default_donate_perc = Decimal(current_app.config.get('default_donate_perc', 0))
    fee_perc = Decimal(current_app.config.get('fee_perc', 0.02))
    # Add custom user percentages that apply into an easy to access dictionary
    custom_percs = {d.user: d.pdonation_perc for d in custom_settings}

    # Track total collections/payments
    collection_total = 0  # Comprised of fees + donations
    payment_total = 0  # Tracks bonuses paid out (from running negative fees)
    # dictionary keyed by user to hold combined percs and amnts
    user_perc_applied = {}
    user_perc = {}
    for user, payout in user_payouts.iteritems():
        # use the custom perc, or fallback to the default
        perc = custom_percs.get(user, default_donate_perc) + fee_perc
        user_perc[user] = perc

        # if the perc is greater than 0 it's considered a collection
        if perc > 0:
            collection = (perc * payout).quantize(current_app.SATOSHI)
            current_app.logger.debug("Collected \t{}\t({}%)\t from\t{}"
                                     .format(collection, perc * 100, user))
            collection_total += collection
            user_payouts[user] -= collection
            user_perc_applied[user] = collection

        # if less than zero it's a payment
        elif perc < 0:
            perc *= -1
            payment = (perc * payout).quantize(current_app.SATOSHI)
            current_app.logger.debug("Paid \t{}\t({}%)\t to\t{}"
                                     .format(payment, perc, user))
            user_payouts[user] += payment
            payment_total += payment
            user_perc_applied[user] = -1 * payment

        # percentages of 0 are no-ops

    swing = payment_total - collection_total
    current_app.logger.info("Paid out {} in bonus payment"
                            .format(payment_total))
    current_app.logger.info("Collected {} in donation + fee payment"
                            .format(collection_total))
    current_app.logger.info("Net income from block {}"
                            .format(swing))

    # Payout the collected amount to the pool
    if swing < 0:
        pool_addr = current_app.config['donate_address']
        user_payouts[pool_addr] = swing * -1
        swing = 0

    # Check this section
    total_payouts = sum(user_payouts.itervalues())
    assert total_payouts == (block.total_value + swing)
    current_app.logger.info("Double check for payout distribution after adding "
                            "fees + donations. Total user payouts {}, total "
                            "block value {}.".format(total_payouts,
                                                     block.total_value))

    # Handle multiple currency payouts
    # =======================================================================
    # Build a dict to hold all currencies a user would like to be paid directly
    user_payable_currencies = {}
    for user in custom_settings:
        # Determine currency abbrev of payout address
        usr_currency = currencies.lookup_address(user).key
        user_payable_currencies[user] = {usr_currency: user}
        # Add any addresses they've configured
        for addr_obj in user.addresses:
            user_payable_currencies[user][addr_obj.currency] = addr_obj.address
        # Add arbitrary payout address if configured
        if user.adonate_addr and user.adonate_perc:
            arb_currency = currencies.lookup_address(user.adonate_addr).key
            user_payable_currencies[user][arb_currency] = user.adonate_addr

    # Convert user_payouts to a dict tracking multiple payouts for a single user
    split_user_payouts = {}
    for user, payout in user_payouts.iteritems():
        split_user_payouts[user] = {user: payout}

    total_splits = 0
    # if they have an arb payout address set up, split their payout
    for p in custom_settings:
        if p.adonate_addr and p.adonate_perc:
            split_amt = split_user_payouts[p.user] * p.adonation_perc
            split_user_payouts[p.user][p.user] -= split_amt
            user_payouts[p.user][p.adonation_addr] = split_amt
            total_splits += 1

    # check to make sure user_payouts total equals split_user_payouts
    new_total_payouts = 0
    for user in split_user_payouts.keys():
        new_total_payouts += sum(split_user_payouts[user].itervalues())
    assert sum(user_payouts.itervalues()) == new_total_payouts
    current_app.logger.info("Successfully split user payouts to include {} "
                            "arbitrary payouts".format(total_splits))

    if simulate:
        out = "\n".join(["\t".join(
            (user, str(sum([amount for amount in payouts.iteritems()]))))
                         for user, payouts in split_user_payouts.iteritems()])
        current_app.logger.debug("Final payout distribution:\nUSR\tAMNT\n{}".format(out))
        db.session.rollback()
    else:
        # record the payout for each user
        for user, payouts in split_user_payouts.iteritems():
            for addr, amount in payouts.iteritems():
                if amount == 0:
                    current_app.logger.info("Skiping zero payout for USR: {} "
                                            "to ADDR: {}".format(user, addr))
                    continue

                # Create a payout record indicating this can be distributed
                if block.currency in user_payable_currencies[user]:
                    p = Payout.create(amount=amount,
                                      block=block,
                                      user=user,
                                      shares=user_shares[user],
                                      perc=user_perc[user],
                                      sharechain_id=sharechain_id,
                                      payout_address=addr)
                    p.payable = True

                # Create a payout entry indicating this needs to be exchanged
                else:
                    p = PayoutExchange.create(amount=amount,
                                              block=block,
                                              user=user,
                                              shares=user_shares[user],
                                              perc=user_perc[user],
                                              sharechain_id=sharechain_id,
                                              payout_address=addr)

        # update the block status and collected amounts
        block.contributed = collection_total
        block.bonus_paid = payment_total


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
