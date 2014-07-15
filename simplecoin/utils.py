import calendar
import datetime
import time
import itertools
import yaml

from sqlalchemy.sql import select
from sqlalchemy.orm import joinedload
from flask import current_app, session, g
from sqlalchemy.sql import func
from cryptokit.base58 import get_bcaddress_version
from cryptokit import bits_to_difficulty
from bitcoinrpc import CoinRPCException

from . import db, cache, root, redis_conn
from .models import (DonationPercent, OneMinuteReject, OneMinuteShare,
                     FiveMinuteShare, FiveMinuteReject, Payout, Block,
                     OneHourShare, OneHourReject, TransactionSummary)


class CommandException(Exception):
    pass


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        current_app.logger.info('{} (args {}, kwargs {}) in {}'
                                .format(method.__name__,
                                        args, kw, time_format(te - ts)))
        return result

    return timed


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
    q = db.session.query(Block).filter_by(user=address, merged_type=None)
    if algo:
        q.filter_by(algo=algo)
    return algo.count()


@cache.memoize(timeout=86400)
def all_time_shares(address):
    shares = db.session.query(OneHourShare).filter_by(user=address)
    return sum([share.value for share in shares])


@cache.memoize(timeout=60)
def last_block_time(algo, merged_type=None):
    return last_block_time_nocache(algo, merged_type=merged_type)


def last_block_time_nocache(algo, merged_type=None):
    """ Retrieves the last time a block was solved using progressively less
    accurate methods. Essentially used to calculate round time.
    TODO XXX: Add pool selector to each of the share queries to grab only x11,
    etc
    """
    last_block = Block.query.filter_by(merged_type=merged_type, algo=algo).order_by(Block.height.desc()).first()
    if last_block:
        return last_block.found_at

    hour = OneHourShare.query.order_by(OneHourShare.time).first()
    if hour:
        return hour.time

    five = FiveMinuteShare.query.order_by(FiveMinuteShare.time).first()
    if five:
        return five.time

    minute = OneMinuteShare.query.order_by(OneMinuteShare.time).first()
    if minute:
        return minute.time

    return datetime.datetime.utcnow()


@cache.memoize(timeout=60)
def last_block_share_id(currency, merged_type=None):
    return last_block_share_id_nocache(currency, merged_type=merged_type)


def last_block_share_id_nocache(algorithm=None, merged_type=None):
    last_block = Block.query.filter_by(merged_type=merged_type).order_by(Block.height.desc()).first()
    if not last_block or not last_block.last_share_id:
        return 0
    return last_block.last_share_id


@cache.memoize(timeout=60)
def last_block_found(algorithm=None, merged_type=None):
    last_block = Block.query.filter_by(merged_type=merged_type).order_by(Block.height.desc()).first()
    if not last_block:
        return None
    return last_block


def last_blockheight(merged_type=None):
    last = last_block_found(merged_type=merged_type)
    if not last:
        return 0
    return last.height


def get_typ(typ, address=None, window=True, worker=None, q_typ=None):
    """ Gets the latest slices of a specific size. window open toggles
    whether we limit the query to the window size or not. We disable the
    window when compressing smaller time slices because if the crontab
    doesn't run we don't want a gap in the graph. This is caused by a
    portion of data that should already be compressed not yet being
    compressed. """
    # grab the correctly sized slices
    base = db.session.query(typ)

    if address is not None:
        base = base.filter_by(user=address)
    if worker is not None:
        base = base.filter_by(worker=worker)
    if q_typ is not None:
        base = base.filter_by(typ=q_typ)
    if window is False:
        return base
    grab = typ.floor_time(datetime.datetime.utcnow()) - typ.window
    return base.filter(typ.time >= grab)


def compress_typ(typ, workers, address=None, worker=None):
    for slc in get_typ(typ, address, window=False, worker=worker):
        if worker is not None:
            slice_dt = typ.floor_time(slc.time)
            stamp = calendar.timegm(slice_dt.utctimetuple())
            workers.setdefault(slc.device, {})
            workers[slc.device].setdefault(stamp, 0)
            workers[slc.device][stamp] += slc.value
        else:
            slice_dt = typ.upper.floor_time(slc.time)
            stamp = calendar.timegm(slice_dt.utctimetuple())
            workers.setdefault(slc.worker, {})
            workers[slc.worker].setdefault(stamp, 0)
            workers[slc.worker][stamp] += slc.value


@cache.cached(timeout=60, key_prefix='pool_hashrate')
def get_pool_hashrate(algo):
    """ Retrieves the pools hashrate average for the last 10 minutes. """
    dt = datetime.datetime.utcnow()
    twelve_ago = dt - datetime.timedelta(minutes=12)
    two_ago = dt - datetime.timedelta(minutes=2)
    ten_min = (OneMinuteShare.query.filter_by(user='pool')
               .filter(OneMinuteShare.time >= twelve_ago, OneMinuteShare.time <= two_ago))
    ten_min = sum([min.value for min in ten_min])
    # shares times hashes per n1 share divided by 600 seconds and 1000 to get
    # khash per second
    return float(ten_min) / 600000


@cache.memoize(timeout=30)
def get_round_shares(algo, merged_type=None):
    """ Retrieves the total shares that have been submitted since the last
    round rollover. """
    suffix = algo
    if merged_type:
        suffix += "_" + merged_type
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
def last_10_shares(user):
    twelve_ago = datetime.datetime.utcnow() - datetime.timedelta(minutes=12)
    two_ago = datetime.datetime.utcnow() - datetime.timedelta(minutes=2)
    minutes = (OneMinuteShare.query.
               filter_by(user=user).filter(OneMinuteShare.time > twelve_ago, OneMinuteShare.time < two_ago))
    if minutes:
        return sum([min.value for min in minutes])
    return 0


def collect_acct_items(address, limit=None, offset=0):
    """ Get account items for a specific user """
    payouts = (Payout.query.filter_by(user=address).join(Payout.block).
               order_by(Block.found_at.desc()).limit(limit).offset(offset))
    return payouts


def collect_user_stats(address):
    """ Accumulates all aggregate user data for serving via API or rendering
    into main user stats page """
    # store all the raw data of we're gonna grab
    workers = {}
    # blank worker template
    def_worker = {'accepted': 0, 'rejected': 0, 'last_10_shares': 0,
                  'online': False, 'server': {}}
    # for picking out the last 10 minutes worth shares...
    now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    twelve_ago = now - datetime.timedelta(minutes=12)
    two_ago = now - datetime.timedelta(minutes=2)
    for m in itertools.chain(get_typ(FiveMinuteShare, address),
                             get_typ(OneMinuteShare, address)):
        workers.setdefault(m.worker, def_worker.copy())
        workers[m.worker]['accepted'] += m.value
        # if in the right 10 minute window add to list
        if m.time >= twelve_ago and m.time < two_ago:
            workers[m.worker]['last_10_shares'] += m.value

    # accumulate reject amount
    for m in itertools.chain(get_typ(FiveMinuteReject, address),
                             get_typ(OneMinuteReject, address)):
        workers.setdefault(m.worker, def_worker.copy())
        workers[m.worker]['rejected'] += m.value

    # pull online status from cached pull direct from powerpool servers
    for name, host in cache.get('addr_online_' + address) or []:
        workers.setdefault(name, def_worker.copy())
        workers[name]['online'] = True
        try:
            workers[name]['server'] = current_app.config['monitor_addrs'][host]['stratum']
        except KeyError:
            workers[name]['server'] = ''

    # pre-calculate a few of the values here to abstract view logic
    for name, w in workers.iteritems():
        workers[name]['last_10_hashrate'] = (shares_to_hashes(w['last_10_shares']) / 1000000) / 600
        if w['accepted'] or w['rejected']:
            workers[name]['efficiency'] = 100.0 * (float(w['accepted']) / (w['accepted'] + w['rejected']))
        else:
            workers[name]['efficiency'] = None

    # sort the workers by their name
    new_workers = []
    for name, data in workers.iteritems():
        new_workers.append(data)
        new_workers[-1]['name'] = name
    new_workers = sorted(new_workers, key=lambda x: x['name'])

    # show their donation percentage
    perc = DonationPercent.query.filter_by(user=address).first()
    if not perc:
        perc = current_app.config.get('default_perc', 0)
    else:
        perc = perc.perc

    user_last_10_shares = last_10_shares(address)
    last_10_hashrate = (shares_to_hashes(user_last_10_shares) / 1000000) / 600

    return dict(workers=new_workers,
                acct_items=collect_acct_items(address, 20),
                donation_perc=perc,
                last_10_shares=user_last_10_shares,
                last_10_hashrate=last_10_hashrate)


def get_pool_eff(timedelta=None):
    rej, acc = get_pool_acc_rej(timedelta)
    # avoid zero division error
    if not rej and not acc:
        return 100
    else:
        return (float(acc) / (acc + rej)) * 100


def shares_to_hashes(shares):
    return float(current_app.config.get('hashes_per_share', 65536)) * shares


##############################################################################
# Message validation and verification functions
##############################################################################
def setfee_command(username, perc):
    perc = round(float(perc), 2)
    if perc > 100.0 or perc < current_app.config['minimum_perc']:
        raise CommandException("Invalid percentage passed!")
    obj = DonationPercent(user=username, perc=perc)
    db.session.merge(obj)
    db.session.commit()


def verify_message(address, message, signature):
    commands = {'SETFEE': setfee_command}
    try:
        lines = message.split("\t")
        parts = lines[0].split(" ")
        command = parts[0]
        args = parts[1:]
        stamp = int(lines[1])
    except (IndexError, ValueError):
        raise CommandException("Invalid information provided in the message "
                               "field. This could be the fault of the bug with "
                               "IE11, or the generated message has an error")

    now = time.time()
    if abs(now - stamp) > current_app.config.get('message_expiry', 840):
        raise CommandException("Signature has expired!")
    if command not in commands:
        raise CommandException("Invalid command given!")

    current_app.logger.info(u"Attempting to validate message '{}' with sig '{}' for address '{}'"
                            .format(message, signature, address))

    try:
        res = coinserv.verifymessage(address, signature, message.encode('utf-8').decode('unicode-escape'))
    except CoinRPCException as e:
        raise CommandException("Rejected by RPC server for reason {}!"
                               .format(e))
    except Exception:
        current_app.logger.error("Coinserver verification error!", exc_info=True)
        raise CommandException("Unable to communicate with coinserver!")

    if res:
        commands[command](address, *args)
    else:
        raise CommandException("Invalid signature! Coinserver returned {}"
                               .format(res))


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
