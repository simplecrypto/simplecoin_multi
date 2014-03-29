import calendar
import time
import yaml
import datetime

from itsdangerous import TimedSerializer
from flask import (current_app, request, render_template, Blueprint, abort,
                   jsonify, g, session, Response)
from lever import get_joined

from .models import (Transaction, OneMinuteShare, Block, Payout, Blob,
                     FiveMinuteShare, OneHourShare, Status, FiveMinuteReject,
                     OneMinuteReject, OneHourReject, DonationPercent,
                     BonusPayout)
from . import db, root, cache
from .utils import (compress_typ, get_typ, verify_message, get_pool_acc_rej,
                    get_pool_eff, last_10_shares, total_earned, total_paid,
                    collect_user_stats, get_adj_round_shares,
                    get_pool_hashrate, last_block_time, get_alerts,
                    last_block_height)


main = Blueprint('main', __name__)


@main.route("/")
def home():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    return render_template('home.html', news=news)


@main.route("/news")
def news():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    return render_template('news.html', news=news)


@main.route("/blocks")
def blocks():
    blocks = db.session.query(Block).order_by(Block.height.desc())
    return render_template('blocks.html', blocks=blocks)


@main.route("/pool_stats")
def pool_stats():
    current_block = db.session.query(Blob).filter_by(key="block").first()
    current_block.data['reward'] = int(current_block.data['reward'])
    blocks = db.session.query(Block).order_by(Block.height.desc()).limit(10)

    reject_total, accept_total = get_pool_acc_rej()
    efficiency = get_pool_eff()

    return render_template('pool_stats.html',
                           blocks=blocks,
                           current_block=current_block,
                           efficiency=efficiency,
                           accept_total=accept_total,
                           reject_total=reject_total)


@main.route("/get_payouts", methods=['POST'])
def get_payouts():
    """ Used by remote procedure call to retrieve a list of transactions to
    be processed. Transaction information is signed for safety. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    s.loads(request.data)

    payouts = (Payout.query.filter_by(transaction_id=None).
               join(Payout.block, aliased=True).filter_by(mature=True))
    bonus_payouts = BonusPayout.query.filter_by(transaction_id=None)
    pids = [(p.user, p.amount, p.id) for p in payouts]
    bids = [(p.user, p.amount, p.id) for p in bonus_payouts]
    return s.dumps([pids, bids])


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
        assert isinstance(data['bids'], list)
        for id in data['pids']:
            assert isinstance(id, int)
        for id in data['bids']:
            assert isinstance(id, int)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to confirm", exc_info=True)
        abort(400)

    coin_trans = Transaction.create(data['coin_txid'])
    db.session.flush()
    Payout.query.filter(Payout.id.in_(data['pids'])).update(
        {Payout.transaction_id: coin_trans.txid}, synchronize_session=False)
    BonusPayout.query.filter(BonusPayout.id.in_(data['bids'])).update(
        {BonusPayout.transaction_id: coin_trans.txid}, synchronize_session=False)
    db.session.commit()
    return s.dumps(True)


@main.before_request
def add_pool_stats():
    g.completed_block_shares = get_adj_round_shares()
    g.round_duration = (datetime.datetime.utcnow() - last_block_time()).total_seconds()
    g.hashrate = get_pool_hashrate()

    blobs = Blob.query.filter(Blob.key.in_(("server", "diff"))).all()
    try:
        server = [b for b in blobs if b.key == "server"][0]
        g.worker_count = int(server.data['stratum_clients'])
    except IndexError:
        g.worker_count = 0
    try:
        diff = float([b for b in blobs if b.key == "diff"][0].data['diff'])
    except IndexError:
        diff = -1
    g.average_difficulty = diff
    g.shares_to_solve = diff * (2 ** 16)
    g.total_round_shares = g.shares_to_solve * current_app.config['last_n']
    g.alerts = get_alerts()


@main.route("/close/<int:id>")
def close_alert(id):
    dismissed_alerts = session.get('dismissed_alerts', [])
    dismissed_alerts.append(id)
    session['dismissed_alerts'] = dismissed_alerts
    return Response('success')


@main.route("/api/pool_stats")
def pool_stats_api():
    ret = {}
    ret['hashrate'] = get_pool_hashrate()
    ret['workers'] = g.worker_count
    ret['completed_shares'] = g.completed_block_shares
    ret['total_round_shares'] = g.total_round_shares
    ret['round_duration'] = g.round_duration
    sps = float(g.completed_block_shares) / g.round_duration
    ret['shares_per_sec'] = sps
    ret['last_block_found'] = last_block_found()
    ret['est_sec_remaining'] = (float(g.shares_to_solve) - g.completed_block_shares) / sps
    return jsonify(**ret)


@main.route("/stats")
def user_stats():
    return render_template('stats.html', page_title='User Stats - Look up statistics for a Dogecoin address')


@main.route("/round_summary")
def summary_page():

    user_shares = cache.get('pplns_user_shares')
    cached_time = cache.get('pplns_cache_time')
    cached_donation = cache.get('user_donations')

    def user_match(user):
        if cached_donation is not None:
            if user in cached_donation:
                return cached_donation[user]
            else:
                return current_app.config['default_perc']

    if cached_time is not None:
        cached_time = cached_time.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")

    if user_shares is None:
        user_list = []
    else:
        user_list = [([shares, user, (65536 * last_10_shares(user[6:]) / 600), user_match(user[6:])]) for user, shares in user_shares.iteritems()]
        user_list = sorted(user_list, key=lambda x: x[0], reverse=True)

    current_block = db.session.query(Blob).filter_by(key="block").first()

    return render_template('round_summary.html',
                           users=user_list,
                           current_block=current_block,
                           cached_time=cached_time)


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


@main.route("/<address>")
def user_dashboard(address=None):
    if len(address) != 34:
        abort(404)

    stats = collect_user_stats(address)

    # reorganize/create the recently viewed
    recent = session.get('recent_users', [])
    if address in recent:
        recent.remove(address)
    recent.insert(0, address)
    session['recent_users'] = recent[:10]

    return render_template('user_stats.html', username=address, **stats)


@main.route("/api/<address>")
def address_api(address):
    if len(address) != 34:
        abort(404)

    stats = collect_user_stats(address)
    stats['acct_items'] = get_joined(stats['acct_items'])
    workers = []
    for name, data in stats['workers'].iteritems():
        workers.append(data)
        workers[-1]['name'] = name
    stats['workers'] = workers
    stats['total_earned'] = float(stats['total_earned'])
    if stats['pplns_cached_time']:
        stats['pplns_cached_time'] = calendar.timegm(stats['pplns_cached_time'].utctimetuple())
    day_shares = stats['last_10_shares'] * 6 * 24
    daily_percentage = float(day_shares) / g.shares_to_solve
    donation_perc = (1 - (stats['donation_perc'] / 100.0))
    rrwd = current_app.config['reward']
    stats['daily_est'] = daily_percentage * rrwd * donation_perc
    stats['est_round_payout'] = (float(stats['round_shares']) / g.total_round_shares) * donation_perc * rrwd
    return jsonify(**stats)


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
        workers[m.worker].setdefault(stamp, 0)
        workers[m.worker][stamp] += m.value
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


@main.route("/faq")
def faq():
    return render_template("faq.html")


@main.route("/set_donation/<address>", methods=['POST', 'GET'])
def set_donation(address):
    vals = request.form
    result = ""
    if request.method == "POST":
        try:
            verify_message(address, vals['message'], vals['signature'])
        except Exception as e:
            current_app.logger.info("Failed to validate!", exc_info=True)
            result = "An error occurred: " + str(e)
        else:
            result = "Successfully changed!"

    perc = DonationPercent.query.filter_by(user=address).first()
    if not perc:
        perc = current_app.config.get('default_perc', 0)
    else:
        perc = perc.perc
    return render_template("set_donation.html", username=address, result=result,
                           perc=perc)
