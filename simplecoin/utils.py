import datetime
import time
import yaml
import json

from flask import current_app, session
from sqlalchemy.exc import SQLAlchemyError
from cryptokit.rpc import CoinRPCException
from decimal import Decimal as dec

from .exceptions import CommandException, InvalidAddressException
from . import db, cache, root, redis_conn, currencies, powerpools, algos
from .models import (ShareSlice, Block, Credit, UserSettings, make_upper_lower,
                     Payout)


class ShareTracker(object):
    def __init__(self, algo):
        self.types = {typ: ShareTypeTracker(typ) for typ in ShareSlice.SHARE_TYPES}
        self.algo = algos[algo]
        self.lowest = None
        self.highest = None

    def count_slice(self, slc):
        self.types[slc.share_type].shares += slc.value
        if not self.lowest or slc.time < self.lowest:
            self.lowest = slc.time
        if not self.highest or slc.end_time > self.highest:
            self.highest = slc.end_time

    @property
    def accepted(self):
        return self.types["acc"].shares

    @property
    def total(self):
        return sum([self.types['dup'].shares, self.types['low'].shares, self.types['stale'].shares, self.types['acc'].shares])

    def hashrate(self, typ="acc"):
        if self.lowest:
            return self.types[typ].shares * self.algo.hashes_per_share / (self.highest - self.lowest).total_seconds()
        else:
            return 0

    @property
    def rejected(self):
        return sum([self.types['dup'].shares, self.types['low'].shares, self.types['stale'].shares])

    @property
    def efficiency(self):
        rej = self.rejected
        acc = float(self.types['acc'].shares)
        if rej:
            return 100.0 * (acc / (rej + acc))
        return 100.0


class ShareTypeTracker(object):
    def __init__(self, share_type):
        self.share_type = share_type
        self.shares = 0

    def __repr__(self):
        return "<ShareTypeTracker 0x{} {} {}>".format(id(self), self.share_type,
                                                      self.shares)

    def __hash__(self):
        return self.share_type.__hash__()


@cache.memoize(timeout=3600)
def get_pool_acc_rej(timedelta=None):
    """ Get accepted and rejected share count totals for the last month """
    if timedelta is None:
        timedelta = datetime.timedelta(days=30)

    # Pull from five minute shares if we're looking at a day timespan
    if timedelta <= datetime.timedelta(days=1):
        rej_typ = FiveMinuteReject
        acc_typ = FiveMinuteShare
    else:
        rej_typ = OneHourReject
        acc_typ = OneHourShare

    one_month_ago = datetime.datetime.utcnow() - timedelta
    rejects = (rej_typ.query.
               filter(rej_typ.time >= one_month_ago).
               filter_by(user="pool_stale"))
    accepts = (acc_typ.query.
               filter(acc_typ.time >= one_month_ago).
               filter_by(user="pool"))
    reject_total = sum([hour.value for hour in rejects])
    accept_total = sum([hour.value for hour in accepts])
    return reject_total, accept_total


@cache.memoize(timeout=3600)
def users_blocks(address, algo=None, merged=None):
    q = db.session.query(Block).filter_by(user=address, merged=False)
    if algo:
        q.filter_by(algo=algo)
    return algo.count()


@cache.memoize(timeout=86400)
def all_time_shares(address):
    shares = db.session.query(ShareSlice).filter_by(user=address)
    return sum([share.value for share in shares])


@cache.memoize(timeout=60)
def last_block_time(algo, merged=False):
    return last_block_time_nocache(algo, merged=merged)


def last_block_time_nocache(algo, merged=False):
    """ Retrieves the last time a block was solved using progressively less
    accurate methods. Essentially used to calculate round time.
    TODO XXX: Add pool selector to each of the share queries to grab only x11,
    etc
    """
    last_block = Block.query.filter_by(merged=merged, algo=algo).order_by(Block.height.desc()).first()
    if last_block:
        return last_block.found_at

    slc = ShareSlice.query.order_by(ShareSlice.time).first()
    if slc:
        return slc.time

    return datetime.datetime.utcnow()


@cache.memoize(timeout=60)
def last_block_share_id(currency, merged=False):
    return last_block_share_id_nocache(currency, merged=merged)


def last_block_share_id_nocache(algorithm=None, merged=False):
    last_block = Block.query.filter_by(merged=merged).order_by(Block.height.desc()).first()
    if not last_block or not last_block.last_share_id:
        return 0
    return last_block.last_share_id


@cache.memoize(timeout=60)
def last_block_found(algorithm=None, merged=False):
    last_block = Block.query.filter_by(merged=merged).order_by(Block.height.desc()).first()
    if not last_block:
        return None
    return last_block


def last_blockheight(merged=False):
    last = last_block_found(merged=merged)
    if not last:
        return 0
    return last.height


@cache.memoize(timeout=60)
def get_pool_hashrate(algo):
    """ Retrieves the pools hashrate average for the last 10 minutes. """
    lower, upper = make_upper_lower(offset=datetime.timedelta(minutes=1))
    ten_min = (ShareSlice.query.filter_by(user='pool', algo=algo)
               .filter(ShareSlice.time >= lower, ShareSlice.time <= upper))
    ten_min = sum([min.value for min in ten_min])
    # shares times hashes per n1 share divided by 600 seconds and 1000 to get
    # khash per second
    return float(ten_min) / 600 * algos[algo].hashes_per_share


@cache.memoize(timeout=30)
def get_round_shares(algo=None, merged=False):
    """ Retrieves the total shares that have been submitted since the last
    round rollover. """
    suffix = algo if algo else merged
    return sum(redis_conn.hvals('current_block_' + suffix)), datetime.datetime.utcnow()


def get_adj_round_shares(khashrate):
    """ Since round shares are cached we still want them to update on every
    page reload, so we extrapolate a new value based on computed average
    shares per second for the round, then add that for the time since we
    computed the real value. """
    round_shares, dt = get_round_shares()
    # # compute average shares/second
    now = datetime.datetime.utcnow()
    sps = float(khashrate * 1000)
    round_shares += int(round((now - dt).total_seconds() * sps))
    return round_shares, sps


@cache.cached(timeout=60, key_prefix='alerts')
def get_alerts():
    return yaml.load(open(root + '/static/yaml/alerts.yaml'))


@cache.memoize(timeout=60)
def last_10_shares(user, algo):
    lower, upper = make_upper_lower(offset=datetime.timedelta(minutes=1))
    minutes = (ShareSlice.query.filter_by(user=user).
               filter(ShareSlice.time > lower, ShareSlice.time < upper))
    if minutes:
        return sum([min.value for min in minutes])
    return 0

@cache.memoize(timeout=60)
def collect_user_credits(address):
    credits = (Credit.query.filter_by(user=address, payout_id=None).
               filter(Credit.block != None).join(Credit.block)
               .order_by(Block.height.desc())).all()
    return credits

@cache.memoize(timeout=60)
def collect_acct_items(address, limit=None, offset=0):
    """ Get account items for a specific user """
    credits = (Credit.query.filter_by(user=address).join(Credit.block).
               order_by(Block.found_at.desc()).limit(limit).offset(offset))
    return credits


def collect_user_stats(user_address):
    """ Accumulates all aggregate user data for serving via API or rendering
    into main user stats page """
    # store all the raw data of we're gonna grab
    workers = {}

    def check_new(user_address, worker, algo):
        """ Setups up an empty worker template. Since anything that has data on
        a worker can create one then it's useful to abstract. """
        key = (user_address, worker, algo)
        if key not in workers:
            workers[key] = {'total_shares': ShareTracker(algo),
                            'last_10_shares': ShareTracker(algo),
                            'online': False,
                            'servers': {},
                            'algo': algo,
                            'name': worker,
                            'address': user_address}
        return workers[key]

    # Get the lower bound for 10 minutes ago
    lower_10, upper_10 = make_upper_lower(offset=datetime.timedelta(minutes=1))

    newest = datetime.datetime.fromtimestamp(0)
    # XXX: Needs to only sum the last 24 hours
    for slc in ShareSlice.get_span(ret_query=True, user=(user_address, )):
        if slc.time > newest:
            newest = slc.time

        worker = check_new(slc.user, slc.worker, slc.algo)
        worker['total_shares'].count_slice(slc)
        if slc.time > lower_10:
            worker['last_10_shares'].count_slice(slc)

    hide_hr = newest < datetime.datetime.utcnow() - datetime.timedelta(seconds=current_app.config['worker_hashrate_fold'])

    # pull online status from cached pull direct from powerpool servers
    for worker, connection_summary in (cache.get('addr_online_' + user_address) or {}).iteritems():
        for ppid, connections in connection_summary.iteritems():
            try:
                powerpool = powerpools[ppid]
            except KeyError:
                continue

            worker = check_new(user_address, worker, powerpool.chain.algo.key)
            worker['online'] = True
            worker['servers'].setdefault(powerpool, 0)
            worker['servers'][powerpool] += 1

    for worker in workers.itervalues():
        worker['status'] = redis_conn.get("status_{address}_{name}".format(**worker))
        if worker['status']:
            worker['status'] = json.loads(worker['status'])
            worker['status_stale'] = False
            worker['status_time'] = datetime.datetime.utcnow()
            try:
                worker['total_hashrate'] = sum([gpu['MHS av'] for gpu in worker['status']['gpus']]) * 1000000
            except Exception:
                worker['total_hashrate'] = -1

            try:
                worker['wu'] = sum(
                    [(gpu['Difficulty Accepted'] / gpu['Device Elapsed']) * 60
                     for gpu in worker['status']['gpus']])
            except KeyError:
                worker['wu'] = 0

            try:
                worker['wue'] = worker['wu'] / (worker['total_hashrate'] / 1000)
            except ZeroDivisionError:
                worker['wue'] = 0.0

            ver = worker['status'].get('v', '0.2.0').split('.')
            try:
                worker['status_version'] = [int(part) for part in ver]
            except ValueError:
                worker['status_version'] = "Unsupp"

    # Could definitely be better... Makes a list of the dictionary keys sorted
    # by the worker name, then generates a list of dictionaries using the list
    # of keys
    workers = [workers[key] for key in sorted(workers.iterkeys(), key=lambda tpl: tpl[1])]

    settings = UserSettings.query.filter_by(user=user_address).first()

    # Generate payout history and stats for earnings all time
    earning_summary = {}
    def_earnings = dict(sent=dec('0'), earned=dec('0'), unconverted=dec('0'), immature=dec('0'), currency=None)
    # Go through already grouped aggregates
    payouts = Payout.query.filter_by(user=user_address).order_by(Payout.created_at).all()
    for payout in payouts:
        summary = earning_summary.setdefault(payout.payout_currency, def_earnings.copy())
        if payout.transaction_id:  # Mark sent if there's a txid attached
            summary['sent'] += payout.amount
        else:
            summary['earned'] += payout.amount

    # Loop through all unaggregated credits to find the rest
    credits = collect_user_credits(user_address)

    for credit in credits:
        # Group by their desired currency
        summary = earning_summary.setdefault(credit.block.currency, def_earnings.copy())

        # For non-traded values run an estimate calculation
        if credit.type == 1:  # CreditExchange
            if not credit.block.mature:
                summary['immature'] += credit.est_value
            elif credit.block.mature and not credit.payable:
                summary['unconverted'] += credit.est_value
            else:
                summary['earned'] += credit.final_amount
        else:
            if not credit.block.mature:
                summary['immature'] += credit.amount
            else:
                summary['earned'] += credit.amount

    # Set the currency as a value of the summary dictionary so we can convert
    # the dictionary of dictionaries into a list of dictionaries for rendering
    for currency in earning_summary:
        earning_summary[currency]['currency'] = currency

    # Show the user approximate next payout and exchange times
    now = datetime.datetime.now()
    next_exchange = now.replace(minute=0, second=0, microsecond=0, hour=((now.hour + 2) % 23))
    next_payout = now.replace(minute=0, second=0, microsecond=0, hour=0)

    f_perc = dec(current_app.config.get('fee_perc', dec('0.02'))) * 100

    return dict(workers=workers,
                credits=credits[:20],
                payouts=payouts[:20],
                settings=settings,
                next_payout=next_payout,
                earning_summary=earning_summary.values(),
                earning_summary_keys=def_earnings.keys(),
                hide_hr=hide_hr,
                next_exchange=next_exchange,
                f_per=f_perc)


def get_pool_eff(timedelta=None):
    rej, acc = get_pool_acc_rej(timedelta)
    # avoid zero division error
    if not rej and not acc:
        return 100
    else:
        return (float(acc) / (acc + rej)) * 100


def shares_to_hashes(shares):
    return float(current_app.config.get('hashes_per_share', 65536)) * shares


def resort_recent_visit(recent):
    """ Accepts a new dictionary of recent visitors and calculates what
    percentage of your total visits have gone to that address. Used to dim low
    percentage addresses. Also sortes showing most visited on top. """
    # accumulate most visited addr while trimming dictionary. NOT Python3 compat
    session['recent_users'] = []
    for i, (addr, visits) in enumerate(sorted(recent.items(), key=lambda x: x[1], reverse=True)):
        if i > 20:
            del recent[addr]
            continue
        session['recent_users'].append((addr, visits))

    # total visits in the list, for calculating percentage
    total = float(sum([t[1] for t in session['recent_users']]))
    session['recent_users'] = [(addr, (visits / total))
                               for addr, visits in session['recent_users']]


class Benchmark(object):
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, ty, val, tb):
        end = time.time()
        current_app.logger.info("BENCHMARK: {} in {}"
                                .format(self.name, time_format(end - self.start)))
        return False


def time_format(seconds):
    # microseconds
    if seconds <= 1.0e-3:
        return "{:,.4f} us".format(seconds * 1000000.0)
    if seconds <= 1.0:
        return "{:,.4f} ms".format(seconds * 1000.0)
    return "{:,.4f} sec".format(seconds)


def validate_str_perc(perc, round=dec('0.01')):
    """
    The go-to function for all your percentage validation needs.

    Tries to convert a var representing an 0-100 scale percentage into a
    mathematically useful Python Decimal. Default is rounding to 0.01%

    Then checks to ensure decimal is within valid bounds
    """
    # Try to convert to decimal
    try:
        dec_perc = dec(perc).quantize(round) / 100
    except TypeError:
        return False
    else:
        # Check bounds
        if dec_perc > dec('1') or dec_perc < dec('0'):
            return False
        else:
            return dec_perc


##############################################################################
# Message validation and verification functions
##############################################################################
def validate_message_vals(address, **kwargs):
    set_addrs = kwargs['SET_ADDR']
    del_addrs = kwargs['DEL_ADDR']
    pdonate_perc = kwargs['SET_PDONATE_PERC']
    spayout_perc = kwargs['SET_SPAYOUT_PERC']
    spayout_addr = kwargs['SET_SPAYOUT_ADDR']
    spayout_curr = kwargs['SET_SPAYOUT_CURR']
    del_spayout_addr = kwargs['DEL_SPAYOUT_ADDR']
    anon = kwargs['MAKE_ANON']

    # Make sure all addresses are valid
    for curr, addr in set_addrs.iteritems():
        try:
            curr_ver = currencies.validate_bc_address(addr)
        except InvalidAddressException:
            raise CommandException("Invalid {} address passed!".format(curr))
        try:
            curr_obj = currencies[curr]
        except Exception:
            raise CommandException("{} is not configured".format(curr))
        # Be a bit extra paranoid
        if not curr_ver in curr_obj.address_version:
            raise CommandException("\'{}\' is not a valid {} "
                                   "address".format(addr, curr))

    # Make sure split payout currency addr is valid and matches the address
    if spayout_addr:
        try:
            curr = currencies.lookup_payable_addr(spayout_addr)
        except Exception:
            raise CommandException("Invalid currency address passed for "
                                   "split payout!")

        if not curr.key == spayout_curr:
            raise CommandException("Split address \'{}\' is not a valid {} "
                                   "address".format(spayout_addr, spayout_curr))

    # Make sure all percentages are valid
    spayout_perc = validate_str_perc(spayout_perc)
    if spayout_perc is False:
        raise CommandException("Split payout percentage invalid! Check to "
                               "make sure its a value 0-100.")

    pdonate_perc = validate_str_perc(pdonate_perc)
    if pdonate_perc is False:
        raise CommandException("Pool donate percentage invalid! Check to "
                               "make sure its a value 0-100.")

    # Make sure percentages are <= 100
    if pdonate_perc + spayout_perc > 100:
        raise CommandException("Donation percentages cannot total to more than "
                               "100%!")

    # Make sure we have both an arb donate addr + an arb donate % or neither
    if not del_spayout_addr:
        if not spayout_perc >= 0 or spayout_addr is False:
            raise CommandException("Split payout requires both an address"
                                   "and a percentage, or to remove it both "
                                   "must be removed.")
    elif del_spayout_addr:
        if spayout_perc > 0 or spayout_addr:
            raise CommandException("Attempted to perform two conflicting "
                                   "actions with split payout! This is "
                                   "probably our fault - please contact us!")

    # Make sure arb donate addr isn't also the main addr
    if spayout_addr == address:
        raise CommandException("Split payout address must not be the same "
                               "as the main user address")

    return (set_addrs, del_addrs, pdonate_perc, spayout_perc, spayout_addr,
            spayout_curr, del_spayout_addr, anon)


def verify_message(address, curr, message, signature):
    update_dict = {'SET_ADDR': {}, 'DEL_ADDR': [], 'MAKE_ANON': False,
                   'SET_PDONATE_PERC': 0, 'SET_SPAYOUT_ADDR': False,
                   'SET_SPAYOUT_PERC': 0, 'DEL_SPAYOUT_ADDR': False,
                   'SET_SPAYOUT_CURR': False}
    stamp = False
    site = False
    try:
        lines = message.split("\t")
        for line in lines:
            parts = line.split(" ")
            if parts[0] in update_dict:
                if parts[0] == 'SET_ADDR':
                    update_dict.setdefault(parts[0], {})
                    update_dict[parts[0]][parts[1]] = parts[2]
                elif parts[0] == 'DEL_ADDR':
                    update_dict[parts[0]].append(parts[1])
                else:
                    update_dict[parts[0]] = parts[1]
            elif parts[0] == 'Only':
                site = parts[3]
            elif parts[0] == 'Generated':
                time = float(parts[2])
                stamp = datetime.datetime.utcfromtimestamp(time)
            else:
                raise CommandException("Invalid command given! Generate a new "
                                       "message & try again.")
    except (IndexError, ValueError):
        current_app.logger.info("Invalid message provided", exc_info=True)
        raise CommandException("Invalid information provided in the message "
                               "field. This could be the fault of the bug with "
                               "IE11, or the generated message has an error")
    if not stamp:
        raise CommandException("Time stamp not found in message! Generate a new"
                               " message & try again.")

    now = datetime.datetime.utcnow()
    if abs((now - stamp).seconds) > current_app.config.get('message_expiry', 90000):
        raise CommandException("Signature/Message is too old to be accepted! "
                               "Make sure your system clock is set correctly, "
                               "then generate a new message & try again.")

    if not site or site != current_app.config['site_title']:
        raise CommandException("Invalid website! Generate a new message "
                               "& try again.")

    current_app.logger.info(u"Attempting to validate message '{}' with sig '{}' for address '{}'"
                            .format(message, signature, address))

    args = validate_message_vals(address, **update_dict)

    try:
        res = curr.coinserv.verifymessage(address, signature, message.encode('utf-8').decode('unicode-escape'))
    except CoinRPCException as e:
        raise CommandException("Rejected by RPC server for reason {}!"
                               .format(e))
    except Exception:
        current_app.logger.error("Coinserver verification error!", exc_info=True)
        raise CommandException("Unable to communicate with coinserver!")

    if res:
        try:
            UserSettings.update(address, *args)
        except SQLAlchemyError:
            db.session.rollback()
            current_app.logger.error("Failed updating database with new user "
                                     "settings! Message: {}".format(message),
                                     exc_info=True)
            raise CommandException("Error saving new settings to the database!")
        else:
            db.session.commit()
    else:
        raise CommandException("Invalid signature! This is usually caused by"
                               "using the wrong address to sign the message or "
                               "not using the QT wallet. Coinserver returned {}"
                               .format(res))
