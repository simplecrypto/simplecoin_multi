import logging
import datetime
import time
import urllib3
import sqlalchemy
import decorator
import argparse

from decimal import Decimal, getcontext, ROUND_HALF_DOWN, ROUND_DOWN
from flask import current_app
from flask.ext.script import Manager
from cryptokit import bits_to_difficulty
from cryptokit.base58 import address_version
from cryptokit.rpc import CoinRPCException

from simplecoin import (db, cache, redis_conn, create_app, currencies,
                        powerpools, chains, algos)
from simplecoin.utils import last_block_time
from simplecoin.exceptions import RemoteException
from simplecoin.models import (Block, Payout, UserSettings, TradeRequest,
                               PayoutExchange, PayoutAggregate, ShareSlice,
                               BlockPayout, DeviceSlice, make_upper_lower)

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
                                    .format(powerpool.unique_id), exc_info=True)
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

    q = Payout.query.filter_by(payable=True, aggregate_id=None).all()
    for payout in q:
        aggr = get_payout_aggr(payout.payout_currency, payout.user, payout.payout_address)
        payout.aggregate = aggr
        if payout.type == 1:
            aggr.amount += payout.buy_amount
        else:
            aggr.amount += payout.amount

        aggr.count += 1

    # Round down to a payable amount (1 satoshi) + record remainder
    for (currency, user, payout_address), aggr in aggrs.iteritems():
        getcontext().rounding = ROUND_DOWN
        amt_payable = aggr.amount.quantize(current_app.SATOSHI)
        user_extra = aggr.amount - amt_payable
        aggr.amount = amt_payable

        if user_extra > 0:
            # Generate a new payout to catch fractional amounts in the next payout
            p = Payout(user=user,
                       amount=user_extra,
                       fee_perc=0,
                       pd_perc=0,
                       payout_currency=currency,
                       payout_address=payout_address,
                       payable=True)
            db.session.add(p)
            current_app.logger.info("Created {} payout for remainder of {} "
                                    "for {}".format(currency, user_extra, user))

    # Generate some simple stats about what we've done
    for (currency, user, payout_address), aggr in aggrs.iteritems():
        adds.setdefault(currency, [0, 0, 0])
        adds[currency][0] += aggr.amount
        adds[currency][1] += aggr.count
        adds[currency][2] += 1

    for curr, (tamount, tcount, count) in adds.iteritems():
        current_app.logger.info(
            "Created {} aggregates paying {} {} for {} payouts"
            .format(count, tamount, curr, tcount))

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
            user.setdefault(slc.algo, [0, 0])
            user[slc.algo][0] += slc.value
            user[slc.algo][1] += 1

    # Loop through and convert a summation of shares into a hashrate. Converts
    # to hashes per second
    for user, algo_shares in users.iteritems():
        for algo_key, (shares, minutes) in algo_shares.items():
            algo_obj = algos[algo_key]
            algo_shares[algo_key] = algo_obj.hashes_per_share * (shares / (minutes * 60))
            algo_shares.setdefault('normalized', 0)
            algo_shares['normalized'] += users[user][algo_key] * algo_obj.normalize_mult

    sorted_users = sorted(users.iteritems(),
                          key=lambda x: x[1]['normalized'],
                          reverse=True)
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
        current_app.logger.info("==== Attempting to process block hash {}".format(hash))
        try:
            payout(key, simulate=simulate)
        except Exception:
            db.session.rollback()
            current_app.logger.error("Unable to payout block {}".format(hash), exc_info=True)
        current_app.logger.info("==== Done processing block hash {}".format(hash))


def payout(redis_key, simulate=False):
    """
    Calculates payouts for users from share records for the latest found block.
    """
    if simulate:
        current_app.logger.debug("Running in simulate mode, no commit will be performed")
        current_app.logger.setLevel(logging.DEBUG)

    data = redis_conn.hgetall(redis_key)
    current_app.logger.debug("Processing block with details {}".format(data))
    merged = data.get('merged', False)
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
    for key, value in data.iteritems():
        if key.startswith("chain_"):
            _, chain_id, key = key.split("_", 2)
            chain_id = int(chain_id)
            chain = chain_data.setdefault(chain_id, {})
            if key == "shares":
                value = Decimal(value)
            chain[key] = value

    current_app.logger.info("Parsed out chain data of {}".format(chain_data))
    block_payouts = []

    # We want to determine each user's shares based on the payout method
    # of that share chain. Those shares will then be paid out proportionally
    # with payout_chain()
    total_shares = sum([dat.get('shares', 0) for dat in chain_data.itervalues()])
    chain_all_paid = Decimal('0')
    for id, data in chain_data.iteritems():
        current_app.logger.info("**** Starting processing chain {}".format(id))
        bp = BlockPayout(chainid=id,
                         block=block,
                         solve_slice=int(data['solve_index']),
                         shares=data['shares'])
        db.session.add(bp)
        block_payouts.append(bp)
        user_shares = chains[id].calc_shares(bp)
        if not user_shares:
            user_shares[block.user] = 1
        chain_total_value = block.total_value * (data['shares'] / total_shares)
        payout_chain(bp, chain_total_value, user_shares, chain_id, simulate=simulate)
        chain_all_paid += chain_total_value
        current_app.logger.info("**** Done processing chain {}".format(id))

    if abs(chain_all_paid - block.total_value) > Decimal('0.000000000001'):
        raise Exception(
            "Total paid out to all chains ({}) is not equal to total block value ({})!"
            .format(chain_all_paid, block.total_value))

    if not simulate:
        db.session.commit()
        redis_conn.delete(redis_key)


def payout_chain(bp, chain_payout_amount, user_shares, sharechain_id, simulate=False):

    # Calculate each user's share
    # =======================================================================
    # Grab total shares to pay out
    total_shares = sum(user_shares.itervalues())
    # Set python Decimal rounding semantic
    getcontext().rounding = ROUND_HALF_DOWN

    if simulate:
        out = "\n".join(
            ["\t".join((user, str((amount * 100) / total_shares),
                        str(((amount * chain_payout_amount) / total_shares).
                            quantize(current_app.SATOSHI)),
                        str(amount))) for user, amount in user_shares.iteritems()])
        current_app.logger.debug("Share distribution:\nUSR\t%\tBLK_PAY\tSHARE"
                                 "\n{}".format(out))

    current_app.logger.debug("Distribute_amnt: {} {}".format(chain_payout_amount, bp.block.currency))
    current_app.logger.debug("Total Shares: {}".format(total_shares))
    current_app.logger.debug("Share Value: {} {}/share".format(chain_payout_amount / total_shares, bp.block.currency))

    # Below calculates the portion going to each miner. Note that the amount
    # is not rounded or truncated - this amount is actually not payable as-is
    user_payouts = {}
    for user, share_count in user_shares.iteritems():
        user_payouts[user] = (share_count * chain_payout_amount) / total_shares

    total_payouts = sum(user_payouts.itervalues())
    if abs(total_payouts - chain_payout_amount) > Decimal('0.000000000001'):
        raise Exception(
            "Total to be paid out to chain ({}) is not ~equal to chain payout "
            "amount ({})!".format(total_payouts, chain_payout_amount))
    current_app.logger.info("Successfully allocated all rewards among {} "
                            "users.".format(len(user_payouts)))

    # Adjust each user's payout to include donations, fees, and bonuses
    # =======================================================================
    # Grab all customized user settings out of the DB
    custom_settings = UserSettings.query.\
        filter(UserSettings.user.in_(user_shares.keys())).all()
    # Grab defaults percs from config
    default_donate_perc = Decimal(current_app.config.get('default_donate_perc', '0'))
    global_default_fee = Decimal(current_app.config.get('fee_perc', '0.02'))
    # Set the fee percentage to the configured sharechain fee, fall back to a
    # global fee, fall back from that to a flat 2% fee
    f_perc = chains[sharechain_id].fee_perc or global_default_fee
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
        d_perc = custom_percs.get(user, default_donate_perc)
        t_perc = d_perc + f_perc
        user_perc[user] = {'d_perc': d_perc, 'f_perc': f_perc}

        # if the perc is greater than 0 it's considered a collection
        if t_perc > 0:
            collection = (t_perc * payout)
            current_app.logger.debug("Collected \t{}\t({}%)\t from\t{}"
                                     .format(collection, t_perc * 100, user))
            collection_total += collection
            user_payouts[user] -= collection
            user_perc_applied[user] = collection

        # if less than zero it's a payment
        elif t_perc < 0:
            t_perc *= -1
            payment = (t_perc * payout)
            current_app.logger.debug("Paid \t{}\t({}%)\t to\t{}"
                                     .format(payment, t_perc, user))
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
                            .format(swing * -1))

    # Payout the collected amount to the pool
    if swing < 0:
        # Check to see if a pool payout addr is specified
        pool_addr = currencies[bp.block.currency]['pool_payout_addr']
        if not pool_addr:
            pool_addr = current_app.config['pool_payout_addr']
            assert currencies[bp.block.currency]['exchangeable'] is True
        user_payouts[pool_addr] = swing * -1
        user_perc[pool_addr] = {'d_perc': 0, 'f_perc': 0}
        swing = 0

    # Check this section
    total_payouts = sum(user_payouts.itervalues())
    if abs(total_payouts - (chain_payout_amount + swing)) > Decimal('0.000000000001'):
        raise Exception(
            "Total to be paid out to chain ({}) is not equal to chain payout "
            "amount ({}) + swing ({})!".format(total_payouts, chain_payout_amount, swing))

    current_app.logger.info("Double check for payout distribution after adding "
                            "fees + donations completed. Total user payouts {}, total "
                            "block value {}.".format(total_payouts,
                                                     chain_payout_amount))

    # Handle multiple currency payouts
    # =======================================================================
    # Build a dict to hold all currencies a user would like to be paid directly
    user_payable_currencies = {}

    # For these next few code blocks we're going to build a dictionary to keep
    # track of which currencies can be paid out directly to a user.
    for user in user_payouts.keys():
        # Determine currency key of user's payout address & add their addr
        try:
            usr_currency = currencies.lookup_payable_addr(user).key
        except Exception:
            current_app.logger.warn('User address {} is not payable!'.format(user))
            # This is an error we cannot handle gracefully, so abort the payout
            raise
        else:
            user_payable_currencies[user] = {usr_currency: user}

    for user in custom_settings:
        # Add any addresses they've configured
        for addr_obj in user.addresses:
            user_payable_currencies[user.user][addr_obj.currency] = addr_obj.address
        # Add split payout address if configured
        if user.spayout_addr and user.spayout_perc and user.spayout_curr:
            user_payable_currencies[user.user][user.spayout_curr] = user.spayout_addr

    # Convert user_payouts to a dict tracking multiple payouts for a single user
    split_user_payouts = {}
    for user, payout in user_payouts.iteritems():
        split_user_payouts[user] = {user: payout}

    total_splits = 0
    # if they have a split payout address set up go ahead and split
    for p in custom_settings:
        if p.spayout_addr and p.spayout_perc and p.spayout_curr:
            split_amt = split_user_payouts[p.user][p.user] * p.spayout_perc
            split_user_payouts[p.user][p.user] -= split_amt
            split_user_payouts[p.user][p.spayout_addr] = split_amt
            total_splits += 1

    # check to make sure user_payouts total equals split_user_payouts
    new_total_payouts = 0
    for user in split_user_payouts.keys():
        new_total_payouts += sum(split_user_payouts[user].itervalues())
    if abs(total_payouts - new_total_payouts) > Decimal('0.000000000001'):
        raise Exception(
            "Total to be paid out to after splitting payouts ({}) is not "
            "close enough to original payout amount ({})!"
            .format(new_total_payouts, total_payouts))
    current_app.logger.info("Successfully split user payouts to include {} "
                            "arbitrary payouts".format(total_splits))

    if simulate:
        out = "Final payout distribution:\nUSR\tAMNT\tADDR"
        for user, payouts in split_user_payouts.iteritems():
            for (addr, amount) in payouts.iteritems():
                out += "\n{}\t{}\t{}".format(user, amount, addr)
        current_app.logger.debug(out)

        db.session.rollback()
        return

    # record the payout for each user
    for user, payouts in split_user_payouts.iteritems():
        for addr, amount in payouts.iteritems():
            if amount == 0:
                current_app.logger.info("Skiping zero payout for USR: {} "
                                        "to ADDR: {}".format(user, addr))
                continue

            # Create a payout record indicating this can be distributed
            if bp.block.currency in user_payable_currencies[user]:
                p = Payout.create(user=user,
                                  amount=amount,
                                  block=bp.block,
                                  fee_perc=user_perc[user]['f_perc'],
                                  pd_perc=user_perc[user]['d_perc'],
                                  sharechain_id=sharechain_id,
                                  payout_currency=bp.block.currency,
                                  payout_address=user_payable_currencies[user][bp.block.currency])
                p.payable = True

            # Create a payout entry indicating this needs to be exchanged
            else:
                curr = currencies.lookup_payable_addr(user).key
                p = PayoutExchange.create(user=user,
                                          amount=amount,
                                          block=bp.block,
                                          fee_perc=user_perc[user]['f_perc'],
                                          pd_perc=user_perc[user]['d_perc'],
                                          sharechain_id=sharechain_id,
                                          payout_currency=curr,
                                          payout_address=addr)
            db.session.add(p)

        # update the block status and collected amounts
        bp.contributed = collection_total
        bp.bonus_paid = payment_total


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
            current_app.logger.warn("Couldn't connect to internal monitor at {}"
                                    .format(powerpool))
            continue
        else:
            raw_servers[powerpool.stratum_address] = data
            servers[powerpool] = dict(workers=data['client_count_authed'],
                                      miners=data['address_count'],
                                      hashrate=data['hps'],
                                      name=powerpool.stratum_address)
            algo_miners.setdefault(powerpool.chain.algo, 0)
            algo_miners[powerpool.chain.algo] += data['address_count']

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
