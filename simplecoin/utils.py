import calendar
import datetime
import time
import itertools

import yaml
from flask import current_app, session
from sqlalchemy.sql import func
from cryptokit.base58 import get_bcaddress_version

from bitcoinrpc import CoinRPCException
from . import db, coinserv, cache, root
from .models import (DonationPercent, OneMinuteReject, OneMinuteShare,
                     FiveMinuteShare, FiveMinuteReject, Payout, BonusPayout,
                     Block, OneHourShare, OneHourReject, Share, Status,
                     MergeAddress)


class CommandException(Exception):
    pass


@cache.memoize(timeout=3600)
def all_blocks(merged_type=None):
    return (db.session.query(Block).filter_by(merged_type=merged_type).
            order_by(Block.height.desc()).all())


@cache.memoize(timeout=3600)
def users_blocks(address, merged=None):
    return db.session.query(Block).filter_by(user=address, merged_type=None).count()


@cache.memoize(timeout=86400)
def all_time_shares(address):
    shares = db.session.query(OneHourShare).filter_by(user=address)
    return sum([shares.value for shares in shares])


@cache.memoize(timeout=60)
def last_block_time(merged_type=None):
    return last_block_time_nocache(merged_type=merged_type)


def last_block_time_nocache(merged_type=None):
    """ Retrieves the last time a block was solved using progressively less
    accurate methods. Essentially used to calculate round time. """
    last_block = Block.query.filter_by(merged_type=merged_type).order_by(Block.height.desc()).first()
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
def last_block_share_id(merged_type=None):
    return last_block_share_id_nocache(merged_type=merged_type)


def last_block_share_id_nocache(merged_type=None):
    last_block = Block.query.filter_by(merged_type=merged_type).order_by(Block.height.desc()).first()
    if not last_block or not last_block.last_share_id:
        return 0
    return last_block.last_share_id


@cache.memoize(timeout=60)
def last_block_found(merged_type=None):
    last_block = Block.query.filter_by(merged_type=merged_type).order_by(Block.height.desc()).first()
    if not last_block:
        return None
    return last_block


def last_blockheight(merged_type=None):
    last = last_block_found(merged_type=merged_type)
    if not last:
        return 0
    return last.height


@cache.cached(timeout=3600, key_prefix='block_stats')
def get_block_stats(average_diff):
    blocks = all_blocks()
    total_shares = 0
    total_difficulty = 0
    total_orphans = 0
    for block in blocks:
        total_shares += block.shares_to_solve
        total_difficulty += block.difficulty
        if block.orphan is True:
            total_orphans += 1

    total_blocks = len(blocks)

    if total_orphans > 0 and total_blocks > 0:
        orphan_perc = (float(total_orphans) / total_blocks) * 100
    else:
        orphan_perc = 0

    if total_shares > 0 and total_difficulty > 0:
        pool_luck = (total_difficulty * (2**32)) / (total_shares * (2**16))
    else:
        pool_luck = 1

    coins_per_day = ((current_app.config['reward'] / (average_diff * (2**32 / 86400))) * 1000000)
    effective_return = (coins_per_day * pool_luck) * ((100 - orphan_perc) / 100)
    return pool_luck, effective_return, orphan_perc


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


@cache.memoize(timeout=10)
def get_round_shares():
    """ Retrieves the total shares that have been submitted since the last
    round rollover. """
    round_shares = (db.session.query(func.sum(Share.shares)).
                    filter(Share.id > last_block_share_id()).scalar() or 0)
    return round_shares, datetime.datetime.utcnow()


def get_adj_round_shares(khashrate):
    """ Since round shares are cached we still want them to update on every
    page reload, so we extrapolate a new value based on computed average
    shares per second for the round, then add that for the time since we
    computed the real value. """
    round_shares, dt = get_round_shares()
    # # compute average shares/second
    now = datetime.datetime.utcnow()
    sps = float(khashrate * 1000) / (2**16)
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
    total_p = (Payout.query.filter_by(user=user).
               join(Payout.block, aliased=True).
               filter_by(orphan=False))
    return int(sum([payout.amount for payout in total_p]))


@cache.memoize(timeout=60)
def total_paid(user):
    total_p = (Payout.query.filter_by(user=user).
               join(Payout.transaction, aliased=True).
               filter_by(confirmed=True))
    return int(sum([tx.amount for tx in total_p]))


@cache.memoize(timeout=60)
def total_bonus(user):
    return int((db.session.query(func.sum(BonusPayout.amount)).
                filter_by(user=user).scalar()) or 0.0)


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


def collect_acct_items(address, limit, offset=0, merged_type=None):
    payouts = (Payout.query.filter_by(user=address, merged_type=merged_type).
               order_by(Payout.id.desc()).limit(limit).offset(offset))
    bonuses = (BonusPayout.query.filter_by(user=address, merged_type=merged_type).
               order_by(BonusPayout.id.desc()).limit(limit).offset(offset))
    return sorted(itertools.chain(payouts, bonuses),
                  key=lambda i: i.created_at, reverse=True)


def collect_user_stats(address):
    """ Accumulates all aggregate user data for serving via API or rendering
    into main user stats page """
    earned = total_earned(address)
    paid = total_paid(address)
    bonus = total_bonus(address)

    balance = earned - paid
    # Add bonuses to total paid amount
    total_payout_amount = (paid + bonus)

    unconfirmed_balance = (Payout.query.filter_by(user=address).
                           join(Payout.block, aliased=True).
                           filter_by(mature=False, orphan=False))
    unconfirmed_balance = sum([payout.amount for payout in unconfirmed_balance])
    balance -= unconfirmed_balance

    pplns_cached_time = cache.get('pplns_cache_time')
    if pplns_cached_time is not None:
        pplns_cached_time.strftime("%Y-%m-%d %H:%M:%S")

    pplns_total_shares = cache.get('pplns_total_shares') or 0
    round_shares = cache.get('pplns_' + address) or 0

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

        try:
            workers[st.worker]['wue'] = workers[st.worker]['wu'] / (workers[st.worker]['total_hashrate']*1000)
        except ZeroDivisionError:
            workers[st.worker]['wue'] = 0.0

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

    # show their merged mining address and collect their payouts
    merged_addrs = []
    merged_accounts = []
    for cfg in current_app.config['merge']:
        if not cfg['enabled']:
            continue
        addr = MergeAddress.query.filter_by(user=address,
                                            merged_type=cfg['currency_name']).first()
        if not addr:
            merged_addrs.append((cfg['currency_name'], cfg['name'], "[not set]"))
        else:
            merged_addrs.append((cfg['currency_name'], cfg['name'], addr.merge_address))
            acct_items = collect_acct_items(addr.merge_address,
                                            20,
                                            merged_type=cfg['currency_name'])
            merged_accounts.append((cfg['currency_name'], cfg['name'], acct_items))

    user_last_10_shares = last_10_shares(address)
    last_10_hashrate = ((user_last_10_shares * 65536.0) / 1000000) / 600

    solved_blocks = users_blocks(address)

    # Calculate 24 hour efficiency for all workers
    total_acc = 0
    total_rej = 0
    for worker in new_workers:
        total_acc += worker['accepted']
        total_rej += worker['rejected']
    if total_acc > 0:
        total_eff = (float(total_acc) / (total_acc + total_rej)) * 100
    else:
        total_eff = 0

    # Grab all the accepted hour shares for the user
    total_shares = all_time_shares(address)

    return dict(workers=new_workers,
                round_shares=round_shares,
                pplns_cached_time=pplns_cached_time,
                pplns_total_shares=pplns_total_shares,
                acct_items=collect_acct_items(address, 20),
                merged_accounts=merged_accounts,
                merged_addrs=merged_addrs,
                total_earned=earned,
                total_paid=total_payout_amount,
                balance=balance,
                donation_perc=perc,
                last_10_shares=user_last_10_shares,
                last_10_hashrate=last_10_hashrate,
                unconfirmed_balance=unconfirmed_balance,
                solved_blocks=solved_blocks,
                total_eff=total_eff,
                total_shares=total_shares)


def get_pool_eff():
    rej, acc = get_pool_acc_rej()
    # avoid zero division error
    if not rej and not acc:
        return 100
    else:
        return (float(acc) / (acc + rej)) * 100

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


def setmerge_command(username, merged_type, merge_address):
    merged_cfg = current_app.config['merged_cfg'].get(merged_type, {})
    try:
        version = get_bcaddress_version(username)
    except Exception:
        version = False
    if (merge_address[0] != merged_cfg['prefix'] or not version):
            raise CommandException("Invalid {merged_type} address! {merged_type} addresses start with a(n) {}."
                                   .format(merged_cfg['prefix'], merged_type=merged_type))

    if not merged_cfg['enabled']:
        raise CommandException("Merged mining not enabled!")

    obj = MergeAddress(user=username, merge_address=merge_address,
                       merged_type=merged_type)
    db.session.merge(obj)
    db.session.commit()


def verify_message(address, message, signature):
    commands = {'SETFEE': setfee_command,
                'SETMERGE': setmerge_command}
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
