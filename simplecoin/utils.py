import calendar
import datetime
import time
import itertools
import yaml

from flask import current_app
from bitcoinrpc import CoinRPCException
from sqlalchemy.sql import func

from . import db, coinserv, cache, root
from .models import (DonationPercent, OneMinuteReject, OneMinuteShare,
                     FiveMinuteShare, FiveMinuteReject, Payout, BonusPayout,
                     Block, OneHourShare, OneHourReject, Share, Status)


class CommandException(Exception):
    pass


@cache.cached(timeout=60, key_prefix='last_block_time')
def last_block_time():
    """ Retrieves the last time a block was solved using progressively less
    accurate methods. Essentially used to calculate round time. """
    last_block = Block.query.order_by(Block.height.desc()).first()
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


@cache.cached(timeout=60, key_prefix='last_block_share_id')
def last_block_share_id():
    last_block = Block.query.order_by(Block.height.desc()).first()
    if not last_block:
        return 0
    return last_block.last_share_id


@cache.cached(timeout=60, key_prefix='last_block_found')
def last_block_found():
    last_block = Block.query.order_by(Block.height.desc()).first()
    if not last_block:
        return 0
    return last_block.height


def get_typ(typ, address, window=True):
    """ Gets the latest slices of a specific size. window open toggles
    whether we limit the query to the window size or not. We disable the
    window when compressing smaller time slices because if the crontab
    doesn't run we don't want a gap in the graph. This is caused by a
    portion of data that should already be compressed not yet being
    compressed. """
    # grab the correctly sized slices
    base = db.session.query(typ).filter_by(user=address)
    if window is False:
        return base
    grab = typ.floor_time(datetime.datetime.utcnow()) - typ.window
    return base.filter(typ.time >= grab)


def compress_typ(typ, address, workers):
    for slc in get_typ(typ, address, window=False):
        slice_dt = typ.upper.floor_time(slc.time)
        stamp = calendar.timegm(slice_dt.utctimetuple())
        workers.setdefault(slc.worker, {})
        workers[slc.worker].setdefault(stamp, 0)
        workers[slc.worker][stamp] += slc.value


@cache.cached(timeout=60, key_prefix='pool_hashrate')
def get_pool_hashrate():
    """ Retrieves the pools hashrate average for the last 10 minutes. """
    dt = datetime.datetime.utcnow()
    twelve_ago = dt - datetime.timedelta(minutes=12)
    two_ago = dt - datetime.timedelta(minutes=2)
    ten_min = (OneMinuteShare.query.filter_by(user='pool')
               .filter(OneMinuteShare.time >= twelve_ago, OneMinuteShare.time <= two_ago))
    ten_min = sum([min.value for min in ten_min])
    # shares times hashes per n1 share divided by 600 seconds and 1000 to get
    # khash per second
    return (float(ten_min) * (2 ** 16)) / 600000


@cache.cached(timeout=60, key_prefix='round_shares')
def get_round_shares():
    """ Retrieves the total shares that have been submitted since the last
    round rollover. """
    round_shares = (db.session.query(func.sum(Share.shares)).
                    filter(Share.id > last_block_share_id()).scalar() or 0)
    return round_shares, datetime.datetime.utcnow()


def get_adj_round_shares():
    """ Since round shares are cached we still want them to update on every
    page reload, so we extrapolate a new value based on computed average
    shares per second for the round, then add that for the time since we
    computed the real value. """
    round_shares, dt = get_round_shares()
    # compute average shares/second
    now = datetime.datetime.utcnow()
    sps = float(round_shares) / (now - last_block_time()).total_seconds()
    round_shares += int(round((now - dt).total_seconds() * sps))
    return round_shares


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


@cache.memoize(timeout=60)
def total_earned(user):
    return (db.session.query(func.sum(Payout.amount)).
            filter_by(user=user).scalar() or 0.0)


@cache.memoize(timeout=60)
def total_paid(user):
    total_p = (Payout.query.filter_by(user=user).
               join(Payout.transaction, aliased=True).
               filter_by(confirmed=True))
    return sum([tx.amount for tx in total_p])


@cache.memoize(timeout=60)
def total_bonus(user):
    return (db.session.query(func.sum(BonusPayout.amount)).
            filter_by(user=user).scalar() or 0.0)


@cache.cached(timeout=3600, key_prefix='get_pool_acc_rej')
def get_pool_acc_rej():
    rejects = db.session.query(OneHourReject).order_by(OneHourReject.time.asc())
    reject_total = sum([hour.value for hour in rejects])
    accepts = db.session.query(OneHourShare.value)
    # if we found rejects, set the earliest accepted share to consider as the
    # same time we've recieved a reject. This is a hack since we didn't
    # start tracking rejected shares until a few weeks after accepted...
    if reject_total:
        accepts = accepts.filter(OneHourShare.time >= rejects[0].time)
    accept_total = sum([hour.value for hour in accepts])
    return reject_total, accept_total


def collect_user_stats(address):
    """ Accumulates all aggregate user data for serving via API or rendering
    into main user stats page """
    earned = total_earned(address)
    paid = total_paid(address)
    bonus = total_bonus(address)

    balance = float(earned) - paid
    # Add bonuses to total paid amount
    total_payout_amount = (paid + bonus)

    unconfirmed_balance = (Payout.query.filter_by(user=address).
                           join(Payout.block, aliased=True).
                           filter_by(mature=False))
    unconfirmed_balance = sum([payout.amount for payout in unconfirmed_balance])
    balance -= unconfirmed_balance

    payouts = Payout.query.filter_by(user=address).order_by(Payout.id.desc()).limit(20)
    bonuses = BonusPayout.query.filter_by(user=address).order_by(BonusPayout.id.desc()).limit(20)
    acct_items = sorted(itertools.chain(payouts, bonuses),
                        key=lambda i: i.created_at, reverse=True)
    round_shares = cache.get('pplns_' + address) or 0
    pplns_cached_time = cache.get('pplns_cache_time')
    if pplns_cached_time != None:
        pplns_cached_time.strftime("%Y-%m-%d %H:%M:%S")

    pplns_total_shares = cache.get('pplns_total_shares') or 0

    # store all the raw data of we're gonna grab
    workers = {}
    # blank worker template
    def_worker = {'accepted': 0, 'rejected': 0, 'last_10_shares': 0,
                  'online': False, 'status': None, 'server': {}}
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

    # grab and collect all the ppagent status information for easy use
    for st in Status.query.filter_by(user=address):
        workers.setdefault(st.worker, def_worker.copy())
        workers[st.worker]['status'] = st.parsed_status
        workers[st.worker]['status_stale'] = st.stale
        workers[st.worker]['status_time'] = st.time
        workers[st.worker]['total_hashrate'] = sum([gpu['MHS av'] for gpu in workers[st.worker]['status']['gpus']])
        try:
            workers[st.worker]['wu'] = sum(
                [(gpu['Difficulty Accepted'] / gpu['Device Elapsed']) * 60
                 for gpu in workers[st.worker]['status']['gpus']])
        except KeyError:
            workers[st.worker]['wu'] = 0
        workers[st.worker]['wue'] = workers[st.worker]['wu'] / (workers[st.worker]['total_hashrate']*1000)
        ver = workers[st.worker]['status'].get('v', '0.2.0').split('.')
        try:
            workers[st.worker]['status_version'] = [int(part) for part in ver]
        except ValueError:
            workers[st.worker]['status_version'] = "Unsupp"

    # pull online status from cached pull direct from powerpool servers
    for name, host in cache.get('addr_online_' + address) or []:
        workers.setdefault(name, def_worker.copy())
        workers[name]['online'] = True
        try:
            workers[name]['server'] = current_app.config['monitor_addrs'][host]
        except KeyError:
            workers[name]['server'] = {}

    # pre-calculate a few of the values here to abstract view logic
    for name, w in workers.iteritems():
        workers[name]['last_10_hashrate'] = ((w['last_10_shares'] * 65536.0) / 1000000) / 600
        if w['accepted'] or w['rejected']:
            workers[name]['efficiency'] = 100.0 * (float(w['accepted']) / (w['accepted'] + w['rejected']))
        else:
            workers[name]['efficiency'] = None

    # sort the workers
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
    last_10_hashrate = ((user_last_10_shares * 65536.0) / 1000000) / 600

    return dict(workers=new_workers,
                round_shares=round_shares,
                pplns_cached_time=pplns_cached_time,
                pplns_total_shares=pplns_total_shares,
                acct_items=acct_items,
                total_earned=earned,
                total_paid=total_payout_amount,
                balance=balance,
                donation_perc=perc,
                last_10_shares=user_last_10_shares,
                last_10_hashrate=last_10_hashrate,
                unconfirmed_balance=unconfirmed_balance)


def get_pool_eff():
    rej, acc = get_pool_acc_rej()
    # avoid zero division error
    if not rej and not acc:
        return 100
    else:
        return (float(acc) / (acc + rej)) * 100


def setfee_command(username, perc):
    perc = round(float(perc), 2)
    if perc > 100.0 or perc < current_app.config['minimum_perc']:
        raise CommandException("Invalid perc passed")
    obj = DonationPercent(user=username, perc=perc)
    db.session.merge(obj)
    db.session.commit()


def verify_message(address, message, signature):
    commands = {'SETFEE': setfee_command}
    lines = message.split("\t")
    parts = lines[0].split(" ")
    command = parts[0]
    args = parts[1:]
    try:
        stamp = int(lines[1])
    except ValueError:
        raise Exception("Second line must be integer timestamp!")
    now = time.time()
    if abs(now - stamp) > 820:
        raise Exception("Signature has expired!")

    if command not in commands:
        raise Exception("Invalid command given!")

    current_app.logger.error("Attemting to validate message '{}' with sig '{}' for address '{}'"
                             .format(message, signature, address))

    try:
        res = coinserv.verifymessage(address, signature, message)
    except CoinRPCException:
        raise Exception("Rejected by RPC server!")
    except Exception:
        current_app.logger.error("Coinserver verification error!", exc_info=True)
        raise Exception("Unable to communicate with coinserver!")
    if res:
        try:
            commands[command](address, *args)
        except CommandException:
            raise
        except Exception:
            raise Exception("Invalid arguments provided to command!")
    else:
        raise Exception("Invalid signature! Coinserver returned " + str(res))
