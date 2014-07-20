import calendar
import time
import datetime
import yaml
import json

from flask import (current_app, request, render_template, Blueprint, abort,
                   jsonify, g, session, Response)
from lever import get_joined

from .models import (OneMinuteShare, Block, OneMinuteType, FiveMinuteType,
                     FiveMinuteShare, OneHourShare, DonationPercent,
                     FiveMinuteHashrate, OneMinuteHashrate, OneHourHashrate,
                     OneMinuteTemperature, FiveMinuteTemperature,
                     OneHourTemperature, OneHourType)
from . import db, root, cache
from .utils import (compress_typ, get_typ, verify_message, get_pool_acc_rej,
                    get_pool_eff, last_10_shares, collect_user_stats, get_adj_round_shares,
                    get_pool_hashrate, last_block_time, get_alerts,
                    last_block_found, last_blockheight, resort_recent_visit,
                    collect_acct_items, CommandException,
                    shares_to_hashes)


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
@main.route("/blocks/<currency>")
def blocks(currency=None):
    page = int(request.args.get('page', 0))
    if page < 0:
        page = 0
    offset = page * 100
    blocks = (db.session.query(Block).filter_by(merged_type=currency).
              order_by(Block.height.desc()).offset(offset).limit(100))
    return render_template('blocks.html', blocks=blocks, page=page)


@main.route("/<address>/account")
def account(address, currency=None):
    page = int(request.args.get('page', 0))
    if page < 0:
        page = 0
    offset = page * 100

    acct_items = collect_acct_items(address, limit=100, offset=offset)
    return render_template('account.html', acct_items=acct_items, page=page)


@main.route("/pool_stats")
def pool_stats():
    blocks_show = current_app.config.get('blocks_stats_page', 25)
    current_block = {'reward': cache.get('reward') or 0,
                     'difficulty': cache.get('difficulty') or 0,
                     'height': cache.get('blockheight') or 0}

    try:
        server_status = json.loads(str(cache.get('server_status')))
    except ValueError:
        server_status = None

    blocks = (db.session.query(Block).filter_by(merged_type=None).
              order_by(Block.height.desc()).limit(blocks_show))
    merged_blocks = []
    for cfg in current_app.config['merge']:
        if not cfg['enabled']:
            continue
        blks = (db.session.query(Block).
                filter_by(merged_type=cfg['currency_name']).
                order_by(Block.height.desc()).limit(blocks_show))
        merged_blocks.append((cfg['currency_name'], cfg['name'], blks))
    pool_luck, effective_return, orphan_perc = get_block_stats()
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
                           orphan_perc=orphan_perc,
                           merged_blocks=merged_blocks,
                           server_status=server_status)


@main.before_request
def add_pool_stats():
    g.hashrates = {a: get_pool_hashrate(a) for a in current_app.config['algos']}
    g.worker_count = {a: cache.get('total_workers_{}'.format(a)) or 0 for a in current_app.config['algos']}
    g.alerts = get_alerts()


@main.route("/close/<int:id>")
def close_alert(id):
    dismissed_alerts = session.get('dismissed_alerts', [])
    dismissed_alerts.append(id)
    session['dismissed_alerts'] = dismissed_alerts
    return Response('success')


@main.route("/stats")
def user_stats():
    return render_template('stats.html')


@main.route("/exc_test")
def exception():
    current_app.logger.warn("Exception test!")
    raise Exception()
    return ""


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
    end = ((int(time.time()) // step) * step) - step
    start = end - typ.window.total_seconds() + step

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
    return render_template('user_stats.html',
                           username=address,
                           block_reward=cache.get('reward') or 1,
                           **stats)


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


def handle_message(address):
    alert_cls = "danger"
    result = None
    vals = request.form
    if request.method == "POST":
        try:
            verify_message(address, vals['message'], vals['signature'])
        except CommandException as e:
            result = "Error: {}".format(e)
        except Exception as e:
            current_app.logger.info("Unhandled exception in Command validation",
                                    exc_info=True)
            result = "An unhandled error occurred: {}".format(e)
        else:
            result = "Successfully changed!"
            alert_cls = "success"

    return result, alert_cls


@main.route("/set_donation/<address>", methods=['POST', 'GET'])
def set_donation(address):
    result, alert_cls = handle_message(address)

    perc = DonationPercent.query.filter_by(user=address).first()
    if not perc:
        perc = current_app.config.get('default_perc', 0)
    else:
        perc = perc.perc
    return render_template("set_donation.html",
                           username=address,
                           result=result,
                           alert_cls=alert_cls,
                           perc=perc)


@main.route("/crontabs")
def crontabs():
    stats = {}
    prefix = "cron_last_run_"
    prefix_len = len(prefix)
    for key in cache.cache._client.keys("{}*".format(prefix)):
        stats[key[prefix_len:]] = cache.cache._client.hgetall(key)

    return render_template("crontabs.html", stats=stats)
