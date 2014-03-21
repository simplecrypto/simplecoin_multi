import calendar
import time
import requests
import yaml
import datetime

from itsdangerous import TimedSerializer
from flask import (current_app, request, render_template, Blueprint, abort,
                   jsonify, g, session, Response)
from sqlalchemy.sql import func

from .models import (Transaction, OneMinuteShare, Block, Share, Payout,
                     last_block_share_id, last_block_time, Blob, FiveMinuteShare,
                     OneHourShare, Status, FiveMinuteReject, OneMinuteReject, OneHourReject)
from . import db, root, cache
from simpledoge.utils import compress_typ, get_typ


main = Blueprint('main', __name__)

@main.route("/")
def home():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    return render_template('home.html', news=news)


@main.route("/news")
def news():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    return render_template('news.html', news=news)


@main.route("/pool_stats")
def pool_stats():
    current_block = db.session.query(Blob).filter_by(key="block").first()
    current_block.data['reward'] = int(current_block.data['reward'])
    blocks = db.session.query(Block).order_by(Block.height.desc()).limit(10)

    efficiency = get_efficiency_data()

    return render_template('pool_stats.html',
                           blocks=blocks,
                           current_block=current_block,
                           rejects=efficiency[0],
                           accepts=efficiency[1])

@cache.cached(timeout=3600, key_prefix='get_accepted_rejected')
def get_efficiency_data():
    monthly_rejects = db.session.query(OneHourReject).order_by(OneHourReject.time.asc()).all()
    monthly_reject = sum([hour.value for hour in monthly_rejects])
    monthly_accepts = db.session.query(OneHourShare.value).filter(OneHourShare.time >= monthly_rejects[0].time).all()
    monthly_accept = sum([hour.value for hour in monthly_accepts])
    return [monthly_reject, monthly_accept]


@main.route("/get_payouts", methods=['POST'])
def get_payouts():
    """ Used by remote procedure call to retrieve a list of transactions to
    be processed. Transaction information is signed for safety. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    s.loads(request.data)

    payouts = (Payout.query.filter_by(transaction_id=None).
               join(Payout.block, aliased=True).filter_by(mature=True))
    struct = [(p.user, p.amount, p.id)
              for p in payouts]
    return s.dumps(struct)


@main.route("/confirm_payouts", methods=['POST'])
def confirm_transactions():
    """ Used as a response from an rpc payout system. This will either reset
    the sent status of a list of transactions upon failure on the remote side,
    or create a new CoinTransaction object and link it to the transactions to
    signify that the transaction has been processed. Both request and response
    are signed. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    data = s.loads(request.data)

    # basic checking of input
    try:
        assert len(data['coin_txid']) == 64
        assert isinstance(data['pids'], list)
        for id in data['pids']:
            assert isinstance(id, int)
    except AssertionError:
        abort(400)

    coin_trans = Transaction.create(data['coin_txid'])
    db.session.flush()
    Payout.query.filter(Payout.id.in_(data['pids'])).update(
        {Payout.transaction_id: coin_trans.txid}, synchronize_session=False)
    db.session.commit()
    return s.dumps(True)


@main.before_request
def add_pool_stats():
    g.pool_stats = get_frontpage_data()

    additional_seconds = (datetime.datetime.utcnow() - g.pool_stats[2]).total_seconds()
    ratio = g.pool_stats[0] / g.pool_stats[1]
    additional_shares = ratio * additional_seconds
    g.pool_stats[0] += additional_shares
    g.pool_stats[1] += additional_seconds
    blobs = Blob.query.filter(Blob.key.in_(("server", "diff"))).all()
    server = [b for b in blobs if b.key == "server"][0]
    diff = [b for b in blobs if b.key == "diff"][0].data['diff']
    g.current_difficulty = diff
    g.worker_count = int(server.data['stratum_clients'])

    alerts = yaml.load(open(root + '/static/yaml/alerts.yaml'))
    g.alerts = alerts


@main.route("/close/<int:id>")
def close_alert(id):
    dismissed_alerts = session.get('dismissed_alerts', [])
    dismissed_alerts.append(id)
    session['dismissed_alerts'] = dismissed_alerts
    return Response('success')


@main.route("/api/pool_stats")
def pool_stats_api():
    ret = {}
    ret['hashrate'] = (g.pool_stats[3] * (2**16)) / 600000
    ret['workers'] = g.worker_count
    ret['round_shares'] = round(g.pool_stats[0])
    return jsonify(**ret)


@cache.cached(timeout=60, key_prefix='get_total_n1')
def get_frontpage_data():

    # A bit inefficient, but oh well... Make it better later...
    last_share_id = last_block_share_id()
    last_found_at = last_block_time()
    dt = datetime.datetime.utcnow()
    ten_min = (OneMinuteShare.query.filter_by(user='pool')
               .filter(OneMinuteShare.time >= datetime.datetime.utcnow() - datetime.timedelta(seconds=600))
               .order_by(OneMinuteShare.time.desc())
               .limit(10))
    ten_min = sum([min.value for min in ten_min])
    shares = db.session.query(func.sum(Share.shares)).filter(Share.id > last_share_id).scalar() or 0
    last_dt = (datetime.datetime.utcnow() - last_found_at).total_seconds()
    return [shares, last_dt, dt, ten_min]


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


@main.route("/stats")
def user_stats():
    return render_template('stats.html')


@main.route("/round_summary")
def summary_page():

    user_shares = cache.get('pplns_user_shares')


    cached_time = cache.get('pplns_cache_time')
    if cached_time != None:
        cached_time = cached_time.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
    if user_shares == None:
        print user_shares
        user_list = []
    else:
        user_list = [([shares, user, (65536 * last_10_shares(user[6:]) / 600)]) for user, shares in user_shares.iteritems()]
        user_list = sorted(user_list, key=lambda x: x[0], reverse=True)

    current_block = db.session.query(Blob).filter_by(key="block").first()

    return render_template('round_summary.html', users=user_list, current_block=current_block, cached_time=cached_time)


@main.route("/exc_test")
def exception():
    current_app.logger.warn("Exception test!")
    raise Exception()
    return ""


@main.route("/charity")
def charity_view():
    charities = []
    for info in current_app.config['aliases']:
        info['hashes_per_min'] = ((2 ** 16) * last_10_shares(info['address'])) / 600
        info['total_paid'] = total_paid(info['address'])
        charities.append(info)
    return render_template('charity.html', charities=charities)


@main.route("/<address>/<worker>/details/<int:gpu>")
@main.route("/<address>/details/<int:gpu>", defaults={'worker': ''})
@main.route("/<address>//details/<int:gpu>", defaults={'worker': ''})
def worker_detail(address, worker, gpu):
    status = Status.query.filter_by(user=address, worker=worker).first()
    if status:
        output = status.pretty_json(gpu)
    else:
        output = "Not available"
    return jsonify(output=output)


@cache.memoize(timeout=120)
def workers_online(address):
    """ Returns all workers online for an address """
    client_mon = current_app.config['monitor_addr']+'client/'+address
    try:
        req = requests.get(client_mon)
        data = req.json()
    except Exception:
        return []
    return [w['worker'] for w in data[address]]

@main.route("/<address>")
def user_dashboard(address=None):
    if len(address) != 34:
        abort(404)
    earned = total_earned(address)
    total_paid = (Payout.query.filter_by(user=address).
                  join(Payout.transaction, aliased=True).
                  filter_by(confirmed=True))
    total_paid = sum([tx.amount for tx in total_paid])
    balance = float(earned) - total_paid
    unconfirmed_balance = (Payout.query.filter_by(user=address).
                           join(Payout.block, aliased=True).
                           filter_by(mature=False))
    unconfirmed_balance = sum([payout.amount for payout in unconfirmed_balance])
    balance -= unconfirmed_balance

    payouts = db.session.query(Payout).filter_by(user=address).order_by(Payout.id.desc()).limit(20)
    user_shares = cache.get('pplns_' + address)
    pplns_cached_time = cache.get('pplns_cache_time')
    if pplns_cached_time != None:
        pplns_cached_time.strftime("%Y-%m-%d %H:%M:%S")

    # reorganize/create the recently viewed
    recent = session.get('recent_users', [])
    if address in recent:
        recent.remove(address)
    recent.insert(0, address)
    session['recent_users'] = recent[:10]

    # store all the raw data of we're gonna grab
    workers = {}
    def_worker = {'accepted': 0, 'rejected': 0, 'last_10_shares': 0,
                  'online': False, 'status': None}
    now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    twelve_ago = now - datetime.timedelta(minutes=12)
    two_ago = now - datetime.timedelta(minutes=2)
    for m in get_typ(FiveMinuteShare, address).all() + get_typ(OneMinuteShare, address).all():
        workers.setdefault(m.worker, def_worker.copy())
        workers[m.worker]['accepted'] += m.value
        if m.time >= twelve_ago and m.time < two_ago:
            workers[m.worker]['last_10_shares'] += m.value

    for m in get_typ(FiveMinuteReject, address).all() + get_typ(OneMinuteReject, address).all():
        workers.setdefault(m.worker, def_worker.copy())
        workers[m.worker]['rejected'] += m.value

    for st in Status.query.filter_by(user=address):
        workers.setdefault(st.worker, def_worker.copy())
        workers[st.worker]['status'] = st.parsed_status
        workers[st.worker]['status_stale'] = st.stale
        workers[st.worker]['status_time'] = st.time
        ver = workers[st.worker]['status'].get('v', '0.2.0').split('.')
        workers[st.worker]['status_version'] = [int(part) for part in ver]

    for name in workers_online(address):
        workers.setdefault(name, def_worker.copy())
        workers[name]['online'] = True

    return render_template('user_stats.html',
                           username=address,
                           workers=workers,
                           user_shares=user_shares,
                           pplns_cached_time=pplns_cached_time,
                           payouts=payouts,
                           round_reward=250000,
                           total_earned=earned,
                           total_paid=total_paid,
                           balance=balance,
                           unconfirmed_balance=unconfirmed_balance,
                           latest_ppagent=current_app.config['latest_ppagent'])


@main.route("/<address>/clear")
def address_clear(address=None):
    if len(address) != 34:
        abort(404)

    # remove address from the recently viewed
    recent = session.get('recent_users', [])
    if address in recent:
        recent.remove(address)
    session['recent_users'] = recent[:10]

    return jsonify(recent=recent[:10])


@main.route("/<address>/stats")
@main.route("/<address>/stats/<window>")
def address_stats(address=None, window="hour"):
    # store all the raw data of we've grabbed
    workers = {}

    if window == "hour":
        typ = OneMinuteShare
    elif window == "day":
        compress_typ(OneMinuteShare, address, workers)
        typ = FiveMinuteShare
    elif window == "month":
        compress_typ(FiveMinuteShare, address, workers)
        typ = OneHourShare

    for m in get_typ(typ, address):
        stamp = calendar.timegm(m.time.utctimetuple())
        workers.setdefault(m.worker, {})
        workers[m.worker][stamp] = m.value
    step = typ.slice_seconds
    end = ((int(time.time()) // step) * step) - (step * 2)
    start = end - typ.window.total_seconds() + (step * 2)

    if address == "pool" and '' in workers:
        workers['Entire Pool'] = workers['']
        del workers['']

    return jsonify(start=start, end=end, step=step, workers=workers)


@main.errorhandler(Exception)
def handle_error(error):
    current_app.logger.exception(error)
    return render_template("500.html")


@main.route("/guides")
@main.route("/guides/")
def guides_index():
    return render_template("guides/index.html")


@main.route("/guides/<guide>")
def guides(guide):
    return render_template("guides/" + guide + ".html")
