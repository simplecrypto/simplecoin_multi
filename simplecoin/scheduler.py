import logging
import itertools
import datetime
import time
import urllib3
import sqlalchemy
import decorator
import argparse
import decimal

from decimal import Decimal
from flask import current_app
from flask.ext.script import Manager
from cryptokit import bits_to_difficulty
from cryptokit.base58 import address_version
from cryptokit.rpc import CoinRPCException

from simplecoin import (db, cache, redis_conn, create_app, currencies,
                        powerpools, algos, global_config)
from simplecoin.utils import last_block_time
from simplecoin.exceptions import RemoteException, InvalidAddressException
from simplecoin.models import (Block, Credit, UserSettings, TradeRequest,
                               CreditExchange, Payout, ShareSlice, ChainPayout,
                               DeviceSlice, make_upper_lower)

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
            current_app.logger.warn("Unable to connect to PP {} to gather worker summary."
                                    .format(powerpool.full_info()), exc_info=True)
            continue

        for address, connections in data['clients'].iteritems():
            user = users.setdefault('addr_online_' + address, {})
            for connection in connections:
                worker = user.setdefault(connection['worker'], {})
                worker.setdefault(ppid, 0)
                worker[ppid] += 1

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
        user_donations[user.user] = user.pdonation_perc

    cache.set('user_donations', user_donations, timeout=1440 * 60)


@crontab
@SchedulerCommand.command
def create_payouts():
    """
    Groups payable payouts at the end of the day by currency for easier paying
    out and database compaction, allowing deletion of regular payout records.
    """
    grouped_credits = {}

    q = Credit.query.filter_by(payable=True, payout_id=None).all()
    for credit in q:
        key = (credit.currency, credit.user, credit.address)
        lst = grouped_credits.setdefault(key, [])
        lst.append(credit)

    # Round down to a payable amount (1 satoshi) + record remainder
    for (currency, user, address), credits in grouped_credits.iteritems():
        total = sum([credit.payable_amount for credit in credits])
        if total < currencies[currency].minimum_payout:
            current_app.logger.info(
                "Skipping payout gen of {} for {} because insuff minimum"
                .format(currency, user))
            continue

        payout = Payout(currency=currency, user=user, address=address,
                        amount=total, count=len(credits))
        db.session.add(payout)
        db.session.flush()

        for credit in credits:
            credit.payout = payout

        amt_payable = payout.amount.quantize(
            current_app.SATOSHI, rounding=decimal.ROUND_DOWN)
        extra = payout.amount - amt_payable
        payout.amount = amt_payable

        if extra > 0:
            # Generate a new credit to catch fractional amounts in the next
            # payout
            p = Credit(user=user,
                       amount=extra,
                       fee_perc=0,
                       source=3,
                       pd_perc=0,
                       currency=currency,
                       address=address,
                       payable=True)
            db.session.add(p)

        current_app.logger.info(
            "Created payout for {} {} with remainder of {}"
            .format(currency, user, extra))

    db.session.commit()


@crontab
@SchedulerCommand.command
def create_trade_req(typ):
    """
    Takes all the credits in need of exchanging (either buying or selling, not
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

    # Attach unattached credits in need of exchange to a new batch of
    # sellrequests

    q = CreditExchange.query.options(db.joinedload('block'))
    # To create a sell request, we find all the credits with no sell request
    # that are mature
    if typ == "sell":
        q = q.filter_by(sell_req=None).join(Credit.block, aliased=True)
    # To create a buy request, we find all the credits with completed sell
    # requests that are mature
    elif typ == "buy":
        q = (q.filter_by(buy_req=None).
             join(CreditExchange.sell_req, aliased=True).
             filter_by(_status=6).join(Credit.block, aliased=True).
             filter_by(mature=True))
    for credit in q:
        if typ == "sell":
            curr = credit.block.currency
            req = get_trade_req(curr)
            credit.sell_req = req
            # We're selling using the mined currency
            req.quantity += credit.amount
        elif typ == "buy":
            curr = currencies[credit.currency].key
            req = get_trade_req(curr)
            credit.buy_req = req
            # We're buying using the currency from the sell request
            req.quantity += credit.sell_amount

        adds.setdefault(curr, 0)
        adds[curr] += 1

    for curr, req in reqs.iteritems():
        if typ == "buy":
            current_app.logger.info("Created a buy trade request for {} with {} BTC containing {:,} CreditExchanges"
                                    .format(req.currency, req.quantity, adds[curr]))
        else:
            current_app.logger.info("Created a sell trade request for {} {} containing {:,} CreditExchanges"
                                    .format(req.quantity, req.currency, adds[curr]))

    if not reqs:
        current_app.logger.info("No CreditExchange's found to create {} "
                                "requests for".format(typ))

    db.session.commit()


@crontab
@SchedulerCommand.command
def leaderboard():
    users = {}
    lower_10, upper_10 = make_upper_lower(offset=datetime.timedelta(minutes=1))
    for slc in ShareSlice.get_span(ret_query=True, lower=lower_10, upper=upper_10):
        try:
            address_version(slc.user)
        except Exception:
            pass
        else:
            user = users.setdefault(slc.user, {})
            user.setdefault(slc.algo, [0, set()])
            user[slc.algo][0] += slc.value
            user[slc.algo][1].add(slc.time)

    # Loop through and convert a summation of shares into a hashrate. Converts
    # to hashes per second
    for user, algo_shares in users.iteritems():
        for algo_key, (shares, minutes) in algo_shares.items():
            algo_obj = algos[algo_key]
            algo_shares[algo_key] = algo_obj.hashes_per_share * (shares / (len(minutes) * 60))
            algo_shares.setdefault('normalized', 0)
            algo_shares['normalized'] += users[user][algo_key] * algo_obj.normalize_mult

    sorted_users = sorted(users.iteritems(),
                          key=lambda x: x[1]['normalized'],
                          reverse=True)

    # This is really bad.... XXX: Needs rework!
    if users:
        anon = [s.user for s in UserSettings.query.filter(
            UserSettings.user.in_(users.keys())).filter_by(anon=True).all()]
        for i, (user, data) in enumerate(sorted_users):
            if user in anon:
                sorted_users[i] = ("Anonymous", data)

    cache.set("leaderboard", sorted_users, timeout=15 * 60)


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

        if not blockheight:
            current_app.logger.warn("Skipping block state update because we "
                                    "failed trying to poll the RPC!")
            continue

        # Skip checking if height difference isn't sufficient. Avoids polling
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
                for credit in block.credits:
                    if credit.type == 0:
                        credit.payable = True
            # else if the result shows insufficient confirms, mark orphan
            elif output['confirmations'] < currency.block_mature_confirms:
                current_app.logger.info("Block {} occured {} height ago, but not enough confirms. Marking orphan."
                                        .format(block, currency.block_mature_confirms))
                block.orphan = True

        db.session.commit()


@crontab
@SchedulerCommand.option('-ds', '--dont-simulate', default=False, action="store_true")
def generate_credits(dont_simulate=True):
    """ Loops through all the blocks that haven't been credited out and
    attempts to process them """
    simulate = not dont_simulate
    unproc_blocks = redis_conn.keys("unproc_block*")
    for key in unproc_blocks:
        hash = key[13:]
        current_app.logger.info("==== Attempting to process block hash {}".format(hash))
        try:
            credit_block(key, simulate=simulate)
        except Exception:
            db.session.rollback()
            current_app.logger.error("Unable to payout block {}".format(hash), exc_info=True)
        current_app.logger.info("==== Done processing block hash {}".format(hash))


def distributor(amount, splits):
    """ Evenly distributes an amount among a dictionary. Dictionary values
    should be integers (or decimals) representing the ratio the amount should
    be split among. Remainders are simply round robin distributed among splits.
    """
    # Round all values to the precision context
    amount = +amount
    for key in splits:
        splits[key] = +splits[key]

    total_count = sum(splits.itervalues())
    distributed = Decimal('0')
    # We round down here so the distribution cannot sum to more than the total
    # amount
    with decimal.localcontext() as ctx:
        ctx.rounding = decimal.ROUND_DOWN
        for key, val in splits.iteritems():
            assert isinstance(val, Decimal)
            splits[key] = (val / total_count) * amount
            distributed += splits[key]

    # The amount that hasn't been distributed
    remainder = amount - distributed
    if remainder == 0:
        return splits

    # How many parts we can split this remainder into
    divisions = int("".join(map(str, remainder.as_tuple()[1])))
    smallest = remainder / divisions
    # Loop over the dictionary keys in round robin order until we've
    # distributed all the remaining parts
    for i, key in zip(xrange(remainder / smallest), itertools.cycle(splits)):
        splits[key] += smallest

    total_after_rr = sum(splits.itervalues())
    # And it should come out exact!
    if total_after_rr != amount:
        raise Exception("Value after distribution ({}) is not equal to amount"
                        " to be distributed ({})!".format(total_after_rr, amount))

    return splits


def credit_block(redis_key, simulate=False):
    """
    Calculates credits for users from share records for the latest found block.
    """
    # Don't do this truthiness thing
    if simulate is not True:
        simulate = False
    if simulate:
        current_app.logger.debug("Running in simulate mode, no commit will be performed")
        current_app.logger.setLevel(logging.DEBUG)

    data = redis_conn.hgetall(redis_key)
    current_app.logger.debug("Processing block with details {}".format(data))
    merged = bool(int(data.get('merged', False)))
    # If start_time isn't listed explicitly do our best to derive from
    # statistical share records
    if 'start_time' in data:
        time_started = datetime.datetime.utcfromtimestamp(float(data.get('start_time')))
    else:
        time_started = last_block_time(data['algo'], merged=merged)

    block = Block(
        user=data.get('address'),
        height=data['height'],
        total_value=(Decimal(data['total_subsidy']) / 100000000),
        transaction_fees=(Decimal(data['fees']) / 100000000),
        difficulty=bits_to_difficulty(data['hex_bits']),
        hash=data['hash'],
        time_started=time_started,
        currency=data['currency'],
        worker=data.get('worker'),
        found_at=datetime.datetime.utcfromtimestamp(float(data['solve_time'])),
        algo=data['algo'],
        merged=merged)

    db.session.add(block)
    db.session.flush()

    # Parse out chain results from the block key
    chain_data = {}
    chain_default = {'shares': Decimal('0')}

    for key, value in data.iteritems():
        if key.startswith("chain_"):
            _, chain_id, key = key.split("_", 2)
            chain_id = int(chain_id)
            chain = chain_data.setdefault(chain_id, chain_default.copy())
            chain['id'] = chain_id
            if key == "shares":
                value = Decimal(value)
            elif key == "solve_index":
                value = int(value)
            # XXX: Could do extra check for setting duplicate data (overrite) here
            chain[key] = value

    # Objectize the data. Use object to store all information moving forward
    chains = []
    for id, chain in chain_data.iteritems():
        if chain['shares'] == 0:
            continue
        cpo = ChainPayout(chainid=id,
                          block=block,
                          solve_slice=chain['solve_index'],
                          chain_shares=chain['shares'])
        cpo.user_shares = {}
        cpo.credits = {}
        db.session.add(cpo)
        chains.append(cpo)

    # XXX: Would be good to check compositeprimarykey integrity here, but will
    # fail on other constraints
    #db.session.flush()

    # XXX: Change to a tabulate print
    current_app.logger.info("Parsed out chain data of {}".format(chain_data))

    # Distribute total block value among chains
    share_distrib = {chain.chainid: chain.chain_shares for chain in chains}
    distrib = distributor(block.total_value, share_distrib)
    for chain in chains:
        chain.amount = distrib[chain.chainid]

    # Fetch the share distribution for this payout chain
    users = set()
    for chain in chains:
        # Actually fetch the shares from redis!
        chain.user_shares = chain.config_obj.calc_shares(chain)
        # If we have nothing, default to paying out the block finder everything
        if not chain.user_shares:
            chain.user_shares[block.user] = 1
        # Add the users to the set, no dups
        users.update(chain.user_shares.keys())

        # Record how many shares were used to payout
        chain.payout_shares = sum(chain.user_shares.itervalues())

    # Grab all possible user based settings objects for all chains
    custom_settings = {}
    if users:
        custom_settings = {s.user: s for s in UserSettings.query.filter(
            UserSettings.user.in_(users)).all()}

    # XXX: Double check that currency code lookups will work relying on
    # currency obj hashability

    # The currencies that are valid to pay out in from this block. Basically,
    # this block currency + all exchangeable currencies if this block's
    # currency is also exchangeable
    valid_currencies = [block.currency_obj]
    if block.currency_obj.exchangeable is True:
        valid_currencies.extend(currencies.exchangeable_currencies)

    # Get the pools payout information for this block
    global_curr = global_config.pool_payout_currency
    pool_payout = dict(address=block.currency_obj.pool_payout_addr,
                       currency=block.currency_obj,
                       user=global_curr.pool_payout_addr)
    # If this currency has no payout address, switch to global default
    if pool_payout['address'] is None:
        pool_payout['address'] = global_curr.pool_payout_addr
        pool_payout['currency'] = global_curr
        assert block.currency_obj.exchangeable, "Block is un-exchangeable"

    # Double check valid. Paranoid
    address_version(pool_payout['address'])

    def filter_valid(user, address, currency):
        try:
            if isinstance(currency, basestring):
                currency = currencies[currency]
        except KeyError:
            current_app.logger.debug(
                "Converted user {}, addr {}, currency {} => pool addr"
                " because invalid currency"
                .format(user, address, currency))
            return pool_payout
        if currency not in valid_currencies:
            current_app.logger.debug(
                "Converted user {}, addr {}, currency {} => pool addr"
                " because invalid currency"
                .format(user, address, currency))
            return pool_payout

        return dict(address=address, currency=currency, user=user)

    # Parse usernames and user settings to build appropriate credit objects
    for chain in chains:
        for username in chain.user_shares.keys():
            try:
                version = address_version(username)
            except Exception:
                # Give these shares to the pool, invalid address version
                chain.make_credit_obj(shares=chain.user_shares[username],
                                      **pool_payout)
                continue

            currency = currencies.version_map.get(version)
            # Check to see if we need to treat them real special :p
            settings = custom_settings.get(username)
            shares = chain.user_shares.pop(username)
            if settings:
                converted = settings.apply(
                    shares, currency, block.currency, valid_currencies)
                # Check to make sure no funny business
                assert sum(c[2] for c in converted) == shares, "Settings apply function returned bad stuff"
                # Create the separate payout objects from settings return info
                for address, currency, shares in converted:
                    chain.make_credit_obj(
                        shares=shares,
                        **filter_valid(username, address, currency))
            else:
                # (try to) Payout directly to mining address
                chain.make_credit_obj(
                    shares=shares,
                    **filter_valid(username, username, currency))

    # Calculate the portion that each user recieves
    for chain in chains:
        chain.distribute()

    # Another double check
    paid = 0
    fees_collected = 0
    donations_collected = 0
    for chain in chains:
        chain_fee_perc = chain.config_obj.fee_perc
        for credit in chain.credits.itervalues():
            # Skip fees/donations for the pool address
            if credit.user == pool_payout['user']:
                continue

            # To do a final check of payout amount
            paid += credit.amount

            # Fee/donation/bonus lookup
            fee_perc = chain_fee_perc
            donate_perc = Decimal('0')
            settings = custom_settings.get(credit.user)
            if settings:
                donate_perc = settings.pdonation_perc

            # Application
            assert isinstance(fee_perc, Decimal)
            assert isinstance(donate_perc, Decimal)
            fee_amount = credit.amount * fee_perc
            donate_amount = credit.amount * donate_perc
            credit.amount -= fee_amount
            credit.amount -= donate_amount

            # Recording
            credit.fee_perc = int(fee_perc * 100)
            credit.pd_perc = int(donate_perc * 100)

            # Bookkeeping
            donations_collected += donate_amount
            fees_collected += fee_amount

    if fees_collected > 0:
        p = Credit.make_credit(
            user=pool_payout['user'],
            block=block,
            currency=pool_payout['currency'].key,
            source=1,
            address=pool_payout['address'],
            amount=+fees_collected)
        db.session.add(p)

    if donations_collected > 0:
        p = Credit.make_credit(
            user=pool_payout['user'],
            block=block,
            currency=pool_payout['currency'].key,
            source=2,
            address=pool_payout['address'],
            amount=+donations_collected)
        db.session.add(p)

    current_app.logger.info("Collected {} in donation".format(donations_collected))
    current_app.logger.info("Collected {} from fees".format(fees_collected))
    current_app.logger.info("Net swing from block {}"
                            .format(fees_collected + donations_collected))

    pool_key = (pool_payout['user'], pool_payout['address'], pool_payout['currency'])
    for chain in chains:
        if pool_key not in chain.credits:
            continue
        current_app.logger.info(
            "Collected {} from invalid mining addresses on chain {}"
            .format(chain.credits[pool_key].amount, chain.chainid))

    if not simulate:
        db.session.commit()
        redis_conn.delete(redis_key)
    else:
        db.session.rollback()


@crontab
@SchedulerCommand.command
def collect_ppagent_data():
    """ Grabs all the pending ppagent data points """
    _grab_data("temp_*", "temperature")
    _grab_data("hashrate_*", "hashrate")


def _grab_data(prefix, stat):
    proc_name = "processing_{}".format(stat)
    unproc_mins = redis_conn.keys(prefix)
    for key in unproc_mins:
        current_app.logger.info("Processing key {}".format(key))
        try:
            (stamp, ) = key.split("_")[1:]
        except Exception:
            current_app.logger.error("Error processing key {}".format(key),
                                     exc_info=True)
            continue
        minute = datetime.datetime.utcfromtimestamp(float(stamp))
        # To ensure invalid stampt don't get committed
        minute = ShareSlice.floor_time(minute, 0)
        if stamp < (time.time() - 30):
            current_app.logger.info("Skipping timestamp {}, too young".format(minute))
            continue

        redis_conn.rename(key, proc_name)
        for user, value in redis_conn.hgetall(proc_name).iteritems():
            try:
                address, worker, did = user.split("_")
                value = float(value)
                # Megahashes are was cgminer reports
                if stat == "hashrate":
                    value *= 1000000
            except Exception:
                current_app.logger.error("Error processing key {} on hash {}"
                                         .format(user, key), exc_info=True)
                continue

            try:
                slc = DeviceSlice(user=address, time=minute, worker=worker,
                                  device=did, stat=stat, value=value, span=0)
                db.session.add(slc)
                db.session.commit()
            except sqlalchemy.exc.IntegrityError:
                current_app.logger.warn("SQLAlchemy collision", exc_info=True)
                db.session.rollback()
        redis_conn.delete(proc_name)


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
            current_app.logger.info("Skipping timestamp {}, too young"
                                    .format(minute))
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

            if address != "pool":
                try:
                    curr = currencies.lookup_payable_addr(address)
                except InvalidAddressException:
                    curr = None

                if not curr:
                    address = global_config.pool_payout_currency.pool_payout_addr

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
    DeviceSlice.compress(0)
    db.session.commit()


@crontab
@SchedulerCommand.command
def compress_five_minute():
    ShareSlice.compress(1)
    DeviceSlice.compress(1)
    db.session.commit()


@crontab
@SchedulerCommand.command
def server_status():
    """
    Periodicly poll the backend to get number of workers and other general
    status information.
    """
    algo_miners = {}
    servers = {}
    raw_servers = {}
    for powerpool in powerpools.itervalues():
        try:
            data = powerpool.request('')
        except Exception:
            current_app.logger.warn("Couldn't connect to internal monitor {}"
                                    .format(powerpool.full_info()))
            continue
        else:
            raw_servers[powerpool.stratum_address] = data
            servers[powerpool] = dict(workers=data['client_count_authed'],
                                      miners=data['address_count'],
                                      hashrate=data['hps'],
                                      name=powerpool.stratum_address)
            algo_miners.setdefault(powerpool.chain.algo.key, 0)
            algo_miners[powerpool.chain.algo.key] += data['address_count']

    cache.set('raw_server_status', raw_servers, timeout=1200)
    cache.set('server_status', servers, timeout=1200)
    cache.set('total_miners', algo_miners, timeout=1200)


def main():
    parser = argparse.ArgumentParser(prog='simplecoin task scheduler')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    args = parser.parse_args()

    create_app("scheduler", log_level=args.log_level)


if __name__ == "__main__":
    main()
