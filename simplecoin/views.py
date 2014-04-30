import calendar
import time

import datetime

import yaml
from flask import (current_app, request, render_template, Blueprint, abort,
                   jsonify, g, session, Response)
from lever import get_joined

from .models import (OneMinuteShare, Block, OneMinuteType, FiveMinuteType,
                     FiveMinuteShare, OneHourShare, Status, DonationPercent,
                     FiveMinuteHashrate, OneMinuteHashrate, OneHourHashrate, OneMinuteTemperature,
                     FiveMinuteTemperature, OneHourTemperature, OneHourType)
from . import db, root, cache
from .utils import (compress_typ, get_typ, verify_message, get_pool_acc_rej,
                    get_pool_eff, last_10_shares, collect_user_stats, get_adj_round_shares,
                    get_pool_hashrate, last_block_time, get_alerts,
                    last_block_found, last_blockheight, resort_recent_visit,
                    collect_acct_items, all_blocks, get_block_stats)


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
    blocks = all_blocks()
    return render_template('blocks.html', blocks=blocks)


@main.route("/<address>/account")
def account(address):
    return render_template('account.html',
                           acct_items=collect_acct_items(address, None))


@main.route("/pool_stats")
def pool_stats():
    current_block = {'reward': cache.get('reward') or 0,
                     'difficulty': cache.get('difficulty') or 0,
                     'height': cache.get('blockheight') or 0}

    blocks = db.session.query(Block).order_by(Block.height.desc()).limit(10)
    pool_luck, effective_return, orphan_perc = get_block_stats(g.average_difficulty)
    reject_total, accept_total = get_pool_acc_rej()
    efficiency = get_pool_eff()

    return render_template('pool_stats.html',
                           blocks=blocks,
                           current_block=current_block,
                           efficiency=efficiency,
                           accept_total=accept_total,
                           reject_total=reject_total,
                           pool_luck=pool_luck,
                           effective_return=effective_return,
                           orphan_perc=orphan_perc)


@main.route("/network_stats")
def network_stats():
    network_block_time = current_app.config['block_time']
    network_difficulty = cache.get('difficulty') or 0
    network_avg_difficulty = g.average_difficulty or 0
    network_blockheight = cache.get('blockheight') or 0
    network_hashrate = (network_difficulty * (2**32)) / network_block_time

    return render_template('network_stats.html',
                           network_difficulty=network_difficulty,
                           network_avg_difficulty=network_avg_difficulty,
                           network_blockheight=network_blockheight,
                           network_hashrate=network_hashrate,
                           network_block_time=network_block_time)


@main.route("/network_stats/<graph_type>/<window>")
def network_graph_data(graph_type=None, window="hour"):
    if not graph_type:
        return None

    type_map = {'hour': OneMinuteType,
                'month': OneHourType,
                'day': FiveMinuteType}
    typ = type_map[window]
    types = {}

    compress = None
    if window == "day":
        compress = OneMinuteType
    elif window == "month":
        compress = FiveMinuteType

    if compress:
        for slc in get_typ(compress, q_typ=graph_type):
            slice_dt = compress.floor_time(slc.time)
            stamp = calendar.timegm(slice_dt.utctimetuple())
            types.setdefault(slc.typ, {})
            types[slc.typ].setdefault(stamp, 0)
            types[slc.typ][stamp] += slc.value

    for m in get_typ(typ, q_typ=graph_type):
        stamp = calendar.timegm(m.time.utctimetuple())
        types.setdefault(m.typ, {})
        types[m.typ].setdefault(stamp, 0)
        types[m.typ][stamp] += m.value

    step = typ.slice_seconds
    end = ((int(time.time()) // step) * step) - (step * 2)
    start = end - typ.window.total_seconds() + (step * 2)

    return jsonify(start=start, end=end, step=step, workers=types)


@main.before_request
def add_pool_stats():
    try:
        try:
            if len(session['recent_users'][0]) != 2:
                session['recent_users'] = []
        except (KeyError, IndexError):
            pass
    except IndexError:
        pass
    g.completed_block_shares = get_adj_round_shares()
    g.round_duration = (datetime.datetime.utcnow() - last_block_time()).total_seconds()
    g.hashrate = get_pool_hashrate()

    g.worker_count = cache.get('total_workers') or 0
    g.average_difficulty = cache.get('difficulty_avg') or 1
    g.shares_to_solve = g.average_difficulty * (2 ** 16)
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
    ret['last_block_found'] = last_blockheight()
    ret['shares_to_solve'] = g.shares_to_solve
    if sps > 0:
        ret['est_sec_remaining'] = (float(g.shares_to_solve) - g.completed_block_shares) / sps
    else:
        ret['est_sec_remaining'] = 'infinite'
    ret['pool_luck'], ret['effective_return'], ret['orphan_perc'] = get_block_stats(g.average_difficulty)
    return jsonify(**ret)


@main.route("/api/last_block")
def last_block_api():
    b = last_block_found()
    if not b:
        return jsonify()
    return jsonify(difficulty=b.difficulty,
                   duration=str(b.duration),
                   shares_to_solve=b.shares_to_solve,
                   found_by=b.user,
                   luck=b.luck,
                   height=b.height,
                   hash=b.hash)


@main.route("/index.php")
def mpos_pool_stats_api():
    ret = {}
    action = request.args.get('action', 'none')
    api_key = request.args.get('api_key', 'none')
    if (action == 'getpoolstatus') & (api_key in current_app.config['mpos_api_keys']):
        sps = float(g.completed_block_shares) / g.round_duration
        difficulty = cache.get('difficulty') or 0
        blockheight = cache.get('blockheight') or 0
        data = {"pool_name": current_app.config['site_url'],
                "hashrate": round(get_pool_hashrate(), 0),
                "efficiency": round(get_pool_eff(), 2),
                "workers": g.worker_count,
                "currentnetworkblock": blockheight,
                "nextnetworkblock": blockheight+1,
                "lastblock": last_blockheight(),
                "networkdiff": difficulty,
                "esttime": round((float(g.shares_to_solve) - g.completed_block_shares) / sps, 0),
                "estshares": round(g.shares_to_solve, 0),
                "timesincelast": round(g.round_duration, 0),
                "nethashrate": round((difficulty * 2**32) / current_app.config['block_time'], 0)
                }
        ret['getpoolstatus'] = {"version": "0.3", "runtime": 0, "data": data}

    return jsonify(**ret)


@main.route("/stats")
def user_stats():
    return render_template('stats.html')


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

    redacted = set(current_app.config.get('redacted_addresses', set()))
    user_list = []
    total_hashrate = 0.0
    if user_shares is not None:
        for user, shares in user_shares.iteritems():
            user = user[6:]
            hashrate = (65536 * last_10_shares(user) / 600)
            total_hashrate += hashrate
            dat = {'hashrate': hashrate,
                   'shares': shares,
                   'user': user if user not in redacted else None,
                   'donation_perc': user_match(user)}
            user_list.append(dat)
        user_list = sorted(user_list, key=lambda x: x['shares'], reverse=True)

    return render_template('round_summary.html',
                           users=user_list,
                           blockheight=cache.get('blockheight') or 0,
                           cached_time=cached_time,
                           total_hashrate=total_hashrate)


@main.route("/exc_test")
def exception():
    current_app.logger.warn("Exception test!")
    raise Exception()
    return ""


@main.route("/<address>/<worker>/details/<int:gpu>")
@main.route("/<address>/details/<int:gpu>", defaults={'worker': ''})
@main.route("/<address>//details/<int:gpu>", defaults={'worker': ''})
def gpu_detail(address, worker, gpu):
    status = Status.query.filter_by(user=address, worker=worker).first()
    if status:
        output = status.pretty_json(gpu)
    else:
        output = "Not available"
    return jsonify(output=output)


@main.route("/<address>/<worker>")
def worker_detail(address, worker):
    status = Status.query.filter_by(user=address, worker=worker).first()

    return render_template('worker_detail.html',
                           status=status,
                           username=address,
                           worker=worker)


@main.route("/<address>/<worker>/<stat_type>/<window>")
def worker_stats(address=None, worker=None, stat_type=None, window="hour"):

    if not address or not worker or not stat_type:
        return None

    type_lut = {'hash': {'hour': OneMinuteHashrate,
                         'day': FiveMinuteHashrate,
                         'day_compressed': OneMinuteHashrate,
                         'month': OneHourHashrate,
                         'month_compressed': FiveMinuteHashrate},
                'temp': {'hour': OneMinuteTemperature,
                         'day': FiveMinuteTemperature,
                         'day_compressed': OneMinuteTemperature,
                         'month': OneHourTemperature,
                         'month_compressed': FiveMinuteTemperature}}

    # store all the raw data of we've grabbed
    workers = {}

    typ = type_lut[stat_type][window]

    if window == "day":
        compress_typ(type_lut[stat_type]['day_compressed'], workers, address, worker=worker)
    elif window == "month":
        compress_typ(type_lut[stat_type]['month_compressed'], workers, address, worker=worker)

    for m in get_typ(typ, address, worker=worker):
        stamp = calendar.timegm(m.time.utctimetuple())
        if worker is not None or 'undefined':
            workers.setdefault(m.device, {})
            workers[m.device].setdefault(stamp, 0)
            workers[m.device][stamp] += m.value
        else:
            workers.setdefault(m.worker, {})
            workers[m.worker].setdefault(stamp, 0)
            workers[m.worker][stamp] += m.value
    step = typ.slice_seconds
    end = ((int(time.time()) // step) * step) - (step * 2)
    start = end - typ.window.total_seconds() + (step * 2)

    return jsonify(start=start, end=end, step=step, workers=workers)


@main.route("/<address>")
def user_dashboard(address=None):
    if len(address) != 34:
        abort(404)

    stats = collect_user_stats(address)

    # reorganize/create the recently viewed
    recent = session.get('recent_user_counts', {})
    recent.setdefault(address, 0)
    recent[address] += 1
    session['recent_user_counts'] = recent
    resort_recent_visit(recent)
    return render_template('user_stats.html', username=address, **stats)


@main.route("/api/<address>")
def address_api(address):
    if len(address) != 34:
        abort(404)

    stats = collect_user_stats(address)
    stats['acct_items'] = get_joined(stats['acct_items'])
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
    # remove address from the recently viewed
    recent = session.get('recent_users_counts', {})
    try:
        del recent[address]
    except KeyError:
        pass
    resort_recent_visit(recent)

    return jsonify(recent=session['recent_users'])


@main.route("/<address>/stats")
@main.route("/<address>/stats/<window>")
def address_stats(address=None, window="hour"):
    # store all the raw data of we've grabbed
    workers = {}

    if window == "hour":
        typ = OneMinuteShare
    elif window == "day":
        compress_typ(OneMinuteShare, workers, address)
        typ = FiveMinuteShare
    elif window == "month":
        compress_typ(FiveMinuteShare, workers, address)
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
