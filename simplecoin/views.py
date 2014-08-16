import calendar
from decimal import Decimal
import time
import datetime
import yaml
import json

from flask import (current_app, request, render_template, Blueprint, abort,
                   jsonify, g, session, Response)

from .models import (OneMinuteShare, Block,
                     FiveMinuteShare, OneHourShare, UserSettings,
                     FiveMinuteHashrate, OneMinuteHashrate, OneHourHashrate,
                     OneMinuteTemperature, FiveMinuteTemperature,
                     OneHourTemperature, PayoutAddress)
from . import db, root, cache, currencies
from .utils import (compress_typ, get_typ, verify_message,
                    collect_user_stats, get_pool_hashrate, get_alerts,
                    resort_recent_visit, collect_acct_items, CommandException,
                    check_valid_currency, valid_currencies)


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

    return render_template('pool_stats.html',
                           blocks=blocks,
                           current_block=current_block,
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
    # Do some checking to make sure the address is valid + payable
    curr = check_valid_currency(address)
    if curr:
        curr = curr.key

    allowed_currencies = valid_currencies()
    if len(address) != 34 or not curr or curr not in allowed_currencies:
        return render_template('invalid_address.html',
                               allowed_currencies=allowed_currencies)

    stats = collect_user_stats(address)

    # reorganize/create the recently viewed
    recent = session.get('recent_user_counts', {})
    recent.setdefault(address, 0)
    recent[address] += 1
    session['recent_user_counts'] = recent
    resort_recent_visit(recent)
    return render_template('user_stats.html',
                           username=address,
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


def handle_message(address, curr):
    alert_cls = "danger"
    result = None
    vals = request.form
    if request.method == "POST":
        try:
            verify_message(address, curr, vals['message'], vals['signature'])
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


@main.route("/validate_address", methods=['POST', 'GET'])
def validate_address():
    if request.method == "POST":
        addr = request.json
        curr = check_valid_currency(addr[1])
        if not curr:
            return jsonify({addr[0]: False})
        if not curr.key == addr[0]:
            return jsonify({addr[0]: False})
        else:
            return jsonify({addr[0]: True})


@main.route("/generate_message", methods=['POST'])
def generate_message():
    if request.method == "POST":
        vals = request.form

        # build a few dicts for convenience
        raw_addresses = {}
        commands = {'delete_addrs': [], 'add_addrs': {}}
        for k, v in vals.iteritems():
            if k in current_app.config['currencies']:
                raw_addresses[k] = v
            else:
                if k == 'anonymous':
                    v = True
                if k == 'donateAmount':
                    v = float(v)
                commands[k] = v

        # validate the addresses + add appropriate commands
        errors = {}
        for currency, address in raw_addresses.iteritems():
            if address:
                curr = check_valid_currency(address)
                if not curr:
                    errors[currency] = address
                else:
                    commands['add_addrs'][currency] = address
            else:
                commands['delete_addrs'].append(currency)

        if commands['donateAmount'] > 100 or commands['donateAmount'] < 0:
            errors['donateAmount'] = commands['donateAmount']

        if errors:
            return jsonify({'errors': errors})

        # build message
        msg_str = ''
        for command, v in commands.iteritems():
            if command == 'delete_addrs':
                for curr in v:
                    msg_str += 'DELADDR ' + curr + "\t"
            if command == 'add_addrs':
                for curr, addr in v.iteritems():
                    msg_str += 'SETADDR ' + curr + ' ' + addr + "\t"
            if command == 'anonymous':
                    msg_str += 'MAKEANON' + ' TRUE' + "\t"
            if command == 'donateAmount':
                    msg_str += "SETDONATE " + str(v) + "\t"
        msg_str += "Only valid on " + current_app.config['site_title'] + "\t"
        msg_str += "Generated at " + str(datetime.datetime.utcnow()) + " UTC"

        return jsonify({'msg_str': msg_str})


@main.route("/settings/<address>", methods=['POST', 'GET'])
def settings(address):
    curr = check_valid_currency(address)
    if not curr:
        return render_template('invalid_address.html')

    result, alert_cls = handle_message(address, curr)

    user = UserSettings.query.filter_by(user=address).first()

    user_addresses = {}
    if not user:
        d_perc = 0
    else:
        d_perc = user.hr_perc
        for addr in user.addresses:
            user_addresses[addr.currency] = (addr)

    exchangeable_currencies = {}
    unexchangeable_currencies = {}
    for currency, cfg in currencies.iteritems():
        if cfg.exchangeable:
            if currency in user_addresses:
                exchangeable_currencies[currency] = user_addresses[currency]
            else:
                exchangeable_currencies[currency] = ''
        else:
            if currency in user_addresses:
                unexchangeable_currencies[currency] = user_addresses[currency]
            else:
                unexchangeable_currencies[currency] = ''
    exchangeable_currencies.pop(curr.currency_name)

    return render_template("user_settings.html",
                           username=address,
                           result=result,
                           alert_cls=alert_cls,
                           d_perc=d_perc,
                           user_currency=curr.name,
                           user_currency_name=curr.currency_name,
                           anon=user.anon,
                           exchangeable_currencies=exchangeable_currencies,
                           unexchangeable_currencies=unexchangeable_currencies)


@main.route("/crontabs")
def crontabs():
    stats = {}
    prefix = "cron_last_run_"
    prefix_len = len(prefix)
    for key in cache.cache._client.keys("{}*".format(prefix)):
        stats[key[prefix_len:]] = cache.cache._client.hgetall(key)

    return render_template("crontabs.html", stats=stats)
