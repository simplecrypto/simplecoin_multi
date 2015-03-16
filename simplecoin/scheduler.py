import logging
import itertools
import datetime
from pprint import pprint
import time
import simplejson as json
import urllib3
import sqlalchemy
import decorator
import argparse
import decimal
import bz2

from simplecoin import (db, cache, redis_conn, create_app, currencies,
                        powerpools, algos, global_config, chains)
from simplecoin.utils import last_block_time, anon_users, time_format, \
    get_past_chain_profit
from simplecoin.exceptions import RemoteException, InvalidAddressException
from simplecoin.models import (Block, Credit, UserSettings, TradeRequest,
                               CreditExchange, Payout, ShareSlice, ChainPayout,
                               DeviceSlice, make_upper_lower)

from decimal import Decimal
from flask import current_app
from flask.ext.script import Manager
from cryptokit import bits_to_difficulty
from cryptokit.base58 import address_version
from cryptokit.rpc import CoinRPCException

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

    # Update data for viewing in the /crontabs view
    key_name = 'cron_last_run_{}'.format(func.__name__)
    cache.cache._client.hmset(
        key_name, dict(runtime=t, time=int(time.time())))
    cache.cache._client.expire(key_name, 86400)
    return res


@SchedulerCommand.option('-ds', '--dont-simulate', default=False, action="store_true")
@crontab
def credit_cleanup(days_ago=7, batch_size=10000, sleep=1, dont_simulate=True):
    objs_count = 100
    while objs_count:
        t = time.time()
        days_ago_dt = datetime.datetime.utcnow() - datetime.timedelta(days=days_ago)
        # How inefficient? So inefficient...
        objs = (Credit.query.filter(Credit.payout_id != None).
                join(Credit.block, aliased=True).
                filter(Block.found_at < days_ago_dt).
                join(Credit.payout, aliased=True).
                filter(Payout.transaction_id != None).
                limit(batch_size))
        if dont_simulate:
            objs_count = 0
            ids = []
            for obj in objs:
                objs_count += 1
                ids.append(obj.id)
                db.session.delete(obj)
            db.session.commit()
            db.session.expunge_all()
            current_app.logger.info("Deleted {:,} old credits in {}s"
                                    .format(objs_count, time.time() - t))
        else:
            objs_count = objs.count()
            current_app.logger.info("Would've deleted {:,} old credits in {}s"
                                    .format(objs_count, time.time() - t))

        # Try not to bog down processes with cleanup tasks...
        time.sleep(sleep)


@SchedulerCommand.option('-ds', '--dont-simulate', default=False, action="store_true")
@crontab
def share_cleanup(dont_simulate=True):
    """ Runs chain_cleanup on each chain. """
    for chain in chains.itervalues():
        try:
            chain_cleanup(chain, dont_simulate)
        except Exception:
            current_app.logger.exception(
                "Unhandled exception cleaning up chain {}".format(chain.id))


def chain_cleanup(chain, dont_simulate):
    """ Handles removing all redis share slices that we are fairly certain won't
    be needed to credit a block if one were to be solved in the future. """
    if not chain.currencies:
        current_app.logger.warn(
            "Unable to run share slice cleanup on chain {} since currencies "
            "aren't specified!".format(chain.id))
        return

    # Get the current sharechain index from redis
    current_index = int(redis_conn.get("chain_{}_slice_index".format(chain.id)) or 0)
    if not current_index:
        current_app.logger.warn(
            "Index couldn't be determined for chain {}".format(chain.id))
        return

    # Find the maximum average difficulty of all currencies on this sharechain
    max_diff = 0
    max_diff_currency = None
    for currency in chain.currencies:
        currency_data = cache.get("{}_data".format(currency.key))
        if not currency_data or currency_data['difficulty_avg_stale']:
            current_app.logger.warn(
                "Cache doesn't accurate enough average diff for {} to cleanup chain {}"
                .format(currency, chain.id))
            return

        if currency_data['difficulty_avg'] > max_diff:
            max_diff = currency_data['difficulty_avg']
            max_diff_currency = currency

    assert max_diff != 0

    hashes_to_solve = max_diff * (2 ** 32)
    shares_to_solve = hashes_to_solve / chain.algo.hashes_per_share
    shares_to_keep = shares_to_solve * chain.safety_margin
    if chain.type == "pplns":
        shares_to_keep *= chain.last_n
    current_app.logger.info(
        "Keeping {:,} shares based on max diff {} for {} on chain {}"
        .format(shares_to_keep, max_diff, max_diff_currency, chain.id))

    # Delete any shares past shares_to_keep
    found_shares = 0
    empty_slices = 0
    iterations = 0
    for index in xrange(current_index, -1, -1):
        iterations += 1
        slc_key = "chain_{}_slice_{}".format(chain.id, index)
        key_type = redis_conn.type(slc_key)

        # Fetch slice information
        if key_type == "list":
            empty_slices = 0
            # For speed sake, ignore uncompressed slices
            continue
        elif key_type == "hash":
            empty_slices = 0
            found_shares += float(redis_conn.hget(slc_key, "total_shares"))
        elif key_type == "none":
            empty_slices += 1
        else:
            raise Exception("Unexpected slice key type {}".format(key_type))

        if found_shares >= shares_to_keep or empty_slices >= 20:
            break

    if found_shares < shares_to_keep:
        current_app.logger.info(
            "Not enough shares {:,}/{:,} for cleanup on chain {}"
            .format(found_shares, shares_to_keep, chain.id))
        return

    current_app.logger.info("Found {:,} shares after {:,} iterations"
                            .format(found_shares, iterations))

    # Delete all share slices older than the last index found
    oldest_kept = index - 1
    empty_found = 0
    deleted_count = 0
    for index in xrange(oldest_kept, -1, -1):
        if empty_found >= 20:
            current_app.logger.debug("20 empty in a row, exiting")
            break
        key = "chain_{}_slice_{}".format(chain, index)
        if redis_conn.type(key) == "none":
            empty_found += 1
        else:
            empty_found = 0

            if dont_simulate:
                if redis_conn.delete(key):
                    deleted_count += 1
            else:
                current_app.logger.info("Would delete {}".format(key))

    if dont_simulate:
        current_app.logger.info(
            "Deleted {} total share slices from #{:,}->{:,}"
            .format(deleted_count, oldest_kept, index))


@SchedulerCommand.command
@crontab
def cache_profitability():
    """
    Calculates the profitability from recent blocks
    """
    # track chain profits
    chain_profit = {}

    start_time = datetime.datetime.utcnow() - datetime.timedelta(hours=96)

    query_currencies = [c.key for c in currencies.itervalues() if c.mineable and c.sellable]
    blocks = (Block.query.filter(Block.found_at > start_time).
              filter(Block.currency.in_(query_currencies)).all())

    for block in blocks:
        chain_data = block.chain_profitability()
        current_app.logger.info("Get {} from {}".format(chain_data, block))

        for chainid, data in chain_data.iteritems():

            if chainid not in chains:
                current_app.logger.warn(
                    "Chain #{} not configured properly! Skipping it..."
                    .format(chainid))
                continue

            # Set the block for convenience later
            data['block'] = block
            chain_profit.setdefault(chainid, {})
            chain_profit[chainid].setdefault(block.currency_obj, []).append(data)

    for chainid, chain_currencies in chain_profit.iteritems():
        merged_shares = 0
        main_shares = 0
        merged_currencies = 0
        btc_total = 0
        for currency, entries in chain_currencies.iteritems():
            if currency.merged:
                merged_currencies += 1
            for data in entries:
                btc_total += data['btc_total']
                if currency.merged:
                    merged_shares += data['sold_shares']
                else:
                    main_shares += data['sold_shares']

        hps = chains[chainid].algo.hashes_per_share
        if main_shares != 0:
            btc_per = btc_total / (main_shares * hps)
        elif merged_shares != 0:
            btc_per = btc_total / (merged_shares * hps / merged_currencies)
        else:
            btc_per = 0
        btc_per *= 86400  # per day

        current_app.logger.debug("Caching chain #{} with profit {}"
                                 .format(chainid, btc_per))

        cache.set('chain_{}_profitability'.format(chainid),
                  btc_per, timeout=3600 * 8)


@SchedulerCommand.command
@crontab
def update_online_workers():
    """
    Grabs data on all currently connected clients. Forms a dictionary of this form:
        dict(address=dict(worker_name=dict(powerpool_id=connection_count)))
    And caches each addresses connection summary as a single cache key.
    """
    users = {}
    for ppid, powerpool in powerpools.iteritems():
        try:
            data = powerpool.request('clients/')
        except RemoteException:
            current_app.logger.warn("Unable to connect to PP {} to gather worker summary."
                                    .format(powerpool.full_info()), exc_info=True)
            continue

        for address, connections in data['clients'].iteritems():
            user = users.setdefault('addr_online_' + address, {})
            if isinstance(connections, dict):
                connections = connections.itervalues()
            for connection in connections:
                if isinstance(connection, basestring):
                    continue
                worker = user.setdefault(connection['worker'], {})
                worker.setdefault(ppid, 0)
                worker[ppid] += 1

    cache.set_many(users, timeout=660)


@SchedulerCommand.command
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
        user_donations[user.user] = user.pdonation_perc

    cache.set('user_donations', user_donations, timeout=1440 * 60)


@SchedulerCommand.command
@crontab
def create_payouts():
    """
    Groups payable payouts at the end of the day by currency for easier paying
    out and database compaction, allowing deletion of regular payout records.
    """
    grouped_credits = {}
    payout_summary = {}

    q = Credit.query.filter_by(payable=True, payout_id=None).all()
    for credit in q:

        if credit.block and credit.block.orphan:
            current_app.logger.error(
                "Credit {} was marked as both payable, but it's block was "
                "marked orphaned! Aborting...".format(credit.id))
            return

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

        payout_summary.setdefault(currency, 0)
        payout_summary[currency] += payout.amount

    current_app.logger.info("############### SUMMARY OF PAYOUTS GENERATED #####################")
    current_app.logger.info(pprint(payout_summary))

    db.session.commit()


@SchedulerCommand.command
@crontab
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
        q = (q.filter_by(sell_req=None).
             join(CreditExchange.block, aliased=True).
             filter_by(mature=True))
    # To create a buy request, we find all the credits with completed sell
    # requests that are mature
    elif typ == "buy":
        q = (q.filter_by(buy_req=None).
             join(CreditExchange.sell_req, aliased=True).
             filter_by(_status=6).join(CreditExchange.block, aliased=True).
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


@SchedulerCommand.command
@crontab
def leaderboard():
    users = {}
    lower_10, upper_10 = make_upper_lower(offset=datetime.timedelta(minutes=2))
    for slc in ShareSlice.get_span(share_type=("acc", ), ret_query=True, lower=lower_10, upper=upper_10):
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
        anon = anon_users()
        for i, (user, data) in enumerate(sorted_users):
            if user in anon:
                sorted_users[i] = ("Anonymous", data)

    cache.set("leaderboard", sorted_users, timeout=15 * 60)


@SchedulerCommand.command
@crontab
def update_network():
    """
    Queries the RPC servers confirmed to update network stats information.
    """
    for currency in currencies.itervalues():
        if not currency.mineable:
            continue

        try:
            gbt = currency.coinserv.getblocktemplate({})
        except (urllib3.exceptions.HTTPError, CoinRPCException) as e:
            current_app.logger.error("Unable to communicate with {} RPC server: {}"
                                     .format(currency, e))
            continue

        key = "{}_data".format(currency.key)
        block_cache_key = "{}_block_cache".format(currency.key)

        current_data = cache.get(key)
        if current_data and current_data['height'] == gbt['height']:
            # Already have information for this block
            current_app.logger.debug(
                "Not updating {} net info, height {} already recorded."
                .format(currency, current_data['height']))
        else:
            current_app.logger.info(
                "Updating {} net info for height {}.".format(currency, gbt['height']))

        # Six hours worth of blocks. how many we'll keep in the cache
        keep_count = 21600 / currency.block_time

        difficulty = bits_to_difficulty(gbt['bits'])
        cache.cache._client.lpush(block_cache_key, difficulty)
        cache.cache._client.ltrim(block_cache_key, 0, keep_count)
        diff_list = cache.cache._client.lrange(block_cache_key, 0, -1)
        difficulty_avg = sum(map(float, diff_list)) / len(diff_list)

        cache.set(key,
                  dict(height=gbt['height'],
                       difficulty=difficulty,
                       reward=gbt['coinbasevalue'] * current_app.SATOSHI,
                       difficulty_avg=difficulty_avg,
                       difficulty_avg_stale=len(diff_list) < keep_count),
                  timeout=1200)


@SchedulerCommand.option("-b", "--block-id", type=int, dest="block_id")
@crontab
def update_block_state(block_id=None):
    """
    Loops through blocks (default immature and non-orphaned blocks)

    If `block_id` is passed, instead of the checking the default blocks,
    all blocks of the same currency of a >= id will be updated.

    First checks to see if blocks are orphaned,
    then it checks to see if they are now matured.
    """
    heights = {}

    def get_blockheight(currency):
        if currency.key not in heights:
            try:
                heights[currency.key] = currency.coinserv.getblockcount()
            except Exception as e:
                current_app.logger.error(
                    "Unable to communicate with {} RPC server: {}"
                    .format(currency.key, e))
                return None
        return heights[currency.key]

    # Select immature & non-orphaned blocks if none are passed
    if block_id is None:
        blocks = Block.query.filter_by(mature=False, orphan=False).all()
    else:
        block = Block.query.filter_by(id=block_id).one()
        blocks = (Block.query.filter_by(currency=block.currency)
                             .filter(Block.id >= block_id).all())

    for block in blocks:
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
            break

        # Skip checking if height difference isn't sufficient. Avoids polling
        # the RPC server excessively
        if (blockheight - block.height) < currency.block_mature_confirms:
            current_app.logger.debug(
                "Not doing confirm check on block {} since it's not at check "
                "threshold (last height {})".format(block, blockheight))
            continue

        try:
            # Check to see if the block hash exists in the block chain
            output = currency.coinserv.getblock(block.hash)
            current_app.logger.debug(
                "Confirms: {}; Height diff: {}"
                .format(output['confirmations'], blockheight - block.height))
        except urllib3.exceptions.HTTPError as e:
            current_app.logger.error("Unable to communicate with {} RPC server:"
                                     " {}".format(currency.key, e))
            continue
        except CoinRPCException:
            current_app.logger.info(
                "Block {} not in coin database, assume orphan!".format(block))
            block.orphan = True
            block.mature = False
            for credit in block.credits:
                credit.payable = False
        else:
            # if the block has the proper number of confirms
            if output['confirmations'] >= currency.block_mature_confirms:
                current_app.logger.info(
                    "Block {} meets {} confirms, mark mature"
                    .format(block, currency.block_mature_confirms))
                block.mature = True
                block.orhpan = False
                for credit in block.credits:
                    if credit.type == 0:
                        credit.payable = True
            # else if the result shows insufficient confirms, mark orphan
            elif output['confirmations'] < currency.block_mature_confirms:
                current_app.logger.info(
                    "Block {} occured {} height ago, but not enough confirms. "
                    "Marking orphan.".format(block, currency.block_mature_confirms))
                block.orphan = True
                block.mature = False
                for credit in block.credits:
                    credit.payable = False

        db.session.commit()


@SchedulerCommand.option('-ds', '--dont-simulate', default=False, action="store_true")
@crontab
def generate_credits(dont_simulate=True):
    """ Loops through all the blocks that haven't been credited out and
    attempts to process them """
    simulate = not dont_simulate
    unproc_blocks = redis_conn.keys("unproc_block*")
    for key in unproc_blocks:
        hash = key[13:]
        current_app.logger.info("==== Attempting to process block hash {}"
                                .format(hash))
        try:
            credit_block(key, simulate=simulate)
        except Exception:
            db.session.rollback()
            current_app.logger.error("Unable to payout block {}".format(hash),
                                     exc_info=True)
        current_app.logger.info("==== Done processing block hash {}"
                                .format(hash))


def distributor(*args, **kwargs):
    if not kwargs.get('scale'):
        kwargs['scale'] = current_app.MAX_DECIMALS
    return _distributor(*args, **kwargs)


def _distributor(amount, splits, scale=None, addtl_prec=0):
    """ Evenly (exactly) distributes an amount among a dictionary. Dictionary
    values should be integers (or decimals) representing the ratio the amount
    should be split among. Arithmetic will be performed to `scale` decimal
    places. Amount will be rounded down to `scale` number of decimal places
    _before_ distribution. Remainders from distribution will be given to users
    in order of who deserved the largest remainders, albiet in round robin
    fashion. `addtl_prec` allows you to specify additional precision for
    computing share remainders, allowing a higher likelyhood of fair
    distribution of amount remainders among keys. Usually not needed.  """
    scale = int(scale or 28) * -1
    amount = Decimal(amount)

    if not splits:
        raise Exception("Splits cannot be empty!")

    with decimal.localcontext(decimal.BasicContext) as ctx:
        ctx.rounding = decimal.ROUND_DOWN
        smallest = Decimal((0, (1, ), scale))

        # Set our precision for operations to only what we need it to be,
        # nothing more. This garuntees a large enough precision without setting
        # it so high as to waste a ton of CPU power. A real issue with the
        # slowness of Python Decimals
        # get very largest non-decimal value a share might recieve
        largest_val = int(round(amount))
        # convert to length of digits and add the decimal scale
        ctx.prec = len(str(largest_val)) + (scale * -1) + addtl_prec

        # Round the distribution amount to correct scale. We will distribute
        # exactly this much
        total_count = Decimal(sum(splits.itervalues()))
        new_amount = amount.quantize(smallest)
        # Check that after rounding the distribution amount is within 0.001% of
        # desired
        assert abs(amount - new_amount) < (amount / 10000)
        amount = new_amount

        # Count how much we give out, and also the remainders of adjusting to
        # desired scale
        remainders = {}
        total_distributed = 0
        percent = 0
        for key, value in splits.iteritems():
            if isinstance(value, int):
                value = Decimal(value)
            assert isinstance(value, Decimal)
            share = (value / total_count) * amount
            percent += (value / total_count)
            splits[key] = share.quantize(smallest)
            remainders[key] = share - splits[key]
            total_distributed += splits[key]

        # The amount that hasn't been distributed due to rounding down
        count = (amount - total_distributed) / smallest
        assert total_distributed <= amount
        if count != 0:
            # Loop over the dictionary keys in remainder order until we
            # distribute the leftovers
            keylist = sorted(remainders.iterkeys(), key=remainders.get, reverse=True)
            for i, key in zip(xrange(count), itertools.cycle(keylist)):
                splits[key] += smallest

        total = sum(splits.itervalues())
        # Check that we don't have extra decimal places
        assert total.as_tuple().exponent >= scale
        # And it should come out exact!
        if total != amount:
            raise Exception(
                "Value after distribution ({}) is not equal to amount"
                " to be distributed ({})!".format(total, amount))
        return splits


def credit_block(redis_key, simulate=False):
    """
    Calculates credits for users from share records for the latest found block.
    """
    # Don't do this truthiness thing
    if simulate is not True:
        simulate = False
    if simulate:
        current_app.logger.warn(
            "Running in simulate mode, no DB commit will be performed")
        current_app.logger.setLevel(logging.DEBUG)

    data = redis_conn.hgetall(redis_key)
    current_app.logger.debug("Processing block with details {}".format(data))
    merged = bool(int(data.get('merged', False)))
    # If start_time isn't listed explicitly do our best to derive from
    # statistical share records
    if 'start_time' in data:
        time_started = datetime.datetime.utcfromtimestamp(
            float(data.get('start_time')))
    else:
        time_started = last_block_time(data['algo'], merged=merged)

    if data['fees'] == "None":
        data['fees'] = 0

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
    # this block currency + all buyable currencies if this block's currency is
    # sellable
    valid_currencies = [block.currency_obj]
    if block.currency_obj.sellable is True:
        valid_currencies.extend(currencies.buyable_currencies)

    pool_payout = block.currency_obj.pool_payout

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
        for key, credit in chain.credits.items():
            # don't try to payout users with zero payout
            if credit.amount == 0:
                db.session.expunge(credit)
                del chain.credits[key]
                continue

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

    current_app.logger.info("Collected {} {} in donation"
                            .format(donations_collected, block.currency))
    current_app.logger.info("Collected {} {} from fees"
                            .format(fees_collected, block.currency))
    current_app.logger.info(
        "Net swing from block {} {}"
        .format(fees_collected + donations_collected, block.currency))

    pool_key = (pool_payout['user'], pool_payout['address'],
                pool_payout['currency'])

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


@SchedulerCommand.command
@crontab
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
                try:
                    value = float(value)
                except ValueError:
                    if value != "None":
                        current_app.logger.warn(
                            "Got bogus value {} from ppagent for stat {}"
                            .format(value, stat), exc_info=True)
                    continue

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


@SchedulerCommand.command
@crontab
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

            if not address.startswith("pool"):
                try:
                    curr = currencies.lookup_payable_addr(address)
                except InvalidAddressException:
                    curr = None

                if curr is None:
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


@SchedulerCommand.command
@crontab
def compress_slices():
    for chain in chains.itervalues():
        # Get the index of the last inserted share slice on this chain
        last_complete_slice = redis_conn.get("chain_{}_slice_index".format(chain.id))
        if last_complete_slice is None:
            # Chain must not be in use....
            current_app.logger.debug(
                "No slice index for chain {}".format(chain))
            continue
        else:
            last_complete_slice = int(last_complete_slice)

        # Loop thorugh all possible share slice numbers
        empty = 0
        encoding_time = 0.0
        retrieval_time = 0.0
        entry_count = 0
        encoded_size = 0
        original_size = 0
        last_slice = last_complete_slice
        for slc_idx in xrange(last_complete_slice, 0, -1):
            key = "chain_{}_slice_{}".format(chain.id, slc_idx)
            key_type = redis_conn.type(key)

            # Compress if it's a list. This is raw data from powerpools redis
            # reporter
            if key_type == "list":
                # Reduce empty counter, but don't go negative
                empty = max(0, empty - 1)

                # Retrieve the enencoded information from redis
                t = time.time()
                slice_shares = redis_conn.lrange(key, 0, -1)
                this_original_size = int(redis_conn.debug_object(key)['serializedlength'])
                this_retrieval_time = time.time() - t

                # Parse the list into proper python representation
                data = []
                total_shares = 0
                for entry in slice_shares:
                    user, shares = entry.split(":")
                    shares = Decimal(shares)
                    data.append((user, shares))
                    total_shares += shares
                this_entry_count = len(data)

                # serialization and compression
                t = time.time()
                data = json.dumps(data, separators=(',', ':'), use_decimal=True)
                data = bz2.compress(data)
                this_encoding_time = time.time() - t

                # Put all the new data into a temporary key, then atomically
                # replace the old list key. ensures we never loose data, even
                # on failures (exceptions)
                key_compressed = key + "_compressed"
                redis_conn.hmset(key_compressed,
                                 dict(
                                     date=int(time.time()),
                                     data=data,
                                     encoding="bz2json",
                                     total_shares=total_shares)
                                 )
                redis_conn.rename(key_compressed, key)
                this_encoded_size = int(redis_conn.debug_object(key)['serializedlength'])

                last_slice = slc_idx
                # Update all the aggregates
                encoding_time += this_encoding_time
                retrieval_time += this_retrieval_time
                entry_count += this_entry_count
                encoded_size += this_encoded_size
                original_size += this_original_size
                # Print progress
                current_app.logger.info(
                    "Encoded slice #{:,} containing {:,} entries."
                    " retrieval_time: {}; encoding_time: {}; start_size: {:,}; end_size: {:,}; ratio: {}"
                    .format(slc_idx, this_entry_count,
                            time_format(this_retrieval_time),
                            time_format(this_encoding_time),
                            this_original_size, this_encoded_size,
                            float(this_original_size) / (this_encoded_size or 1)))

            # Count an empty entry to detect the end of live slices
            elif key_type == "none":
                empty += 1

            # If we've seen a lot of empty slices, probably nothing else to find!
            if empty >= 20:
                current_app.logger.info(
                    "Ended compression search at {}".format(slc_idx))
                break

        current_app.logger.info(
            "Encoded from slice #{:,} -> #{:,} containing {:,} entries."
            " retrieval_time: {}; encoding_time: {}; start_size: {:,}; end_size: {:,}; ratio: {}"
            .format(last_complete_slice, last_slice, entry_count,
                    time_format(retrieval_time), time_format(encoding_time),
                    original_size, encoded_size,
                    float(original_size) / (encoded_size or 1)))


@SchedulerCommand.command
@crontab
def compress_minute():
    ShareSlice.compress(0)
    DeviceSlice.compress(0)
    db.session.commit()


@SchedulerCommand.command
@crontab
def compress_five_minute():
    ShareSlice.compress(1)
    DeviceSlice.compress(1)
    db.session.commit()


@SchedulerCommand.command
@crontab
def server_status():
    """
    Periodically poll the backend to get number of workers and other general
    status information.
    """
    past_chain_profit = get_past_chain_profit()
    currency_hashrates = {}
    algo_miners = {}
    servers = {}
    raw_servers = {}
    for powerpool in powerpools.itervalues():

        server_default = dict(workers=0,
                              miners=0,
                              hashrate=0,
                              name='???',
                              profit_4d=0,
                              currently_mining='???')

        try:
            data = powerpool.request('')
        except Exception:
            current_app.logger.warn("Couldn't connect to internal monitor {}"
                                    .format(powerpool.full_info()))
            continue
        else:
            raw_servers[powerpool.stratum_address] = data
            status = {'workers': data['client_count_authed'],
                      'miners': data['address_count'],
                      'hashrate': data['hps'],
                      'name': powerpool.stratum_address,
                      'profit_4d': past_chain_profit[powerpool.chain.id]}

            server_default.update(status)
            servers[powerpool.key] = server_default

            algo_miners.setdefault(powerpool.chain.algo.key, 0)
            algo_miners[powerpool.chain.algo.key] += data['address_count']

            if 'last_flush_job' in data and data['last_flush_job'] \
                    and 'currency' in data['last_flush_job']:
                curr = data['last_flush_job']['currency']
                servers[powerpool.key].update({'currently_mining': curr})
                currency_hashrates.setdefault(currencies[curr], 0)
                currency_hashrates[currencies[curr]] += data['hps']
                # Add hashrate to the merged networks too
                if 'merged_networks' in data['last_flush_job']:
                    for currency in data['last_flush_job']['merged_networks']:
                        currency_hashrates.setdefault(currencies[currency], 0)
                        currency_hashrates[currencies[currency]] += data['hps']

    # Set hashrate to 0 if not located
    for currency in currencies.itervalues():
        hashrate = 0
        if currency in currency_hashrates:
            hashrate = currency_hashrates[currency]

        cache.set('hashrate_' + currency.key, hashrate, timeout=120)

    cache.set('raw_server_status', raw_servers, timeout=1200)
    cache.set('server_status', servers, timeout=1200)
    cache.set('total_miners', algo_miners, timeout=1200)


def main():
    parser = argparse.ArgumentParser(prog='simplecoin task scheduler')
    parser.add_argument('-c', '--config', dest='configs', action='append',
                        type=argparse.FileType('r'))
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    args = parser.parse_args()

    app = create_app("scheduler", log_level=args.log_level)
    app.scheduler.start()


if __name__ == "__main__":
    main()
