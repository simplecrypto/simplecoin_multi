import calendar
import time
import datetime
import yaml
import json

from flask import (current_app, request, render_template, Blueprint, jsonify,
                   g, session, Response)

from .models import Block, ShareSlice, UserSettings, PayoutAddress, make_upper_lower, Payout, PayoutAggregate
from . import db, root, cache, currencies
from .utils import (verify_message, collect_user_stats, get_pool_hashrate,
                    get_alerts, resort_recent_visit, collect_acct_items,
                    CommandException)


main = Blueprint('main', __name__)


@main.route("/")
def home():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    payout_currencies = currencies.payout_currencies()
    return render_template('home.html', news=news, payout_currencies=payout_currencies)

@main.route("/configuration_guide")
def configuration_guide():
    return render_template('config_guide_wrapper.html')


@main.route("/news")
def news():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    return render_template('news.html', news=news)


@main.route("/merge_blocks", defaults={"q": Block.merged_type != None})
@main.route("/blocks", defaults={"q": Block.merged_type == None})
@main.route("/blocks/<currency>")
def blocks(q, currency=None):
    page = int(request.args.get('page', 0))
    if page < 0:
        page = 0
    offset = page * 100
    blocks = (db.session.query(Block).filter(q).
              order_by(Block.found_at.desc()).offset(offset).limit(100))
    if currency:
        blocks = blocks.filter_by(currency=currency)
    return render_template('blocks.html', blocks=blocks, page=page)


@main.route("/<address>/account", defaults={'type': 'payout'})
@main.route("/<address>/aggr_account", defaults={'type': 'aggr'})
def account(address, type):
    page = int(request.args.get('page', 0))
    if page < 0:
        page = 0
    offset = page * 100

    if type == "aggr":
        aggrs = (PayoutAggregate.query.filter_by(user=address).join(Payout.block).
                 order_by(PayoutAggregate.created_at.desc()).limit(100).offset(offset))
        return render_template('account.html', aggregates=aggrs, page=page,
                               table="aggregate_table.html")
    else:
        payouts = (Payout.query.filter_by(user=address).join(Payout.block).
                   order_by(Block.found_at.desc()).limit(100).offset(offset))
        return render_template('account.html', payouts=payouts, page=page,
                               table="acct_table.html")


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
              order_by(Block.found_at.desc()).limit(blocks_show))

    merge_blocks = (db.session.query(Block).filter(Block.merged_type != None).
                    order_by(Block.found_at.desc()).limit(blocks_show))

    return render_template('pool_stats.html',
                           blocks=blocks,
                           merge_blocks=merge_blocks,
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


@main.route("/<address>")
def user_dashboard(address):
    # Do some checking to make sure the address is valid + payable
    try:
        currencies.lookup_address(address)
    except AttributeError:
        return render_template('invalid_address.html',
                               allowed_currencies=currencies.payout_currencies())

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


@main.route("/api/shares")
def address_stats():
    window = request.args.get("window", "hour")
    address = request.args['address'].split(",")
    algos = request.args.get('algos', tuple())
    if algos:
        algos = algos.split(",")
    share_types = request.args.get("share_types", "acc").split(",")

    # store all the raw data of we've grabbed
    workers = {}

    if window == "hour":
        span = 0
    elif window == "day":
        span = 1
    elif window == "month":
        span = 2
    span_config = ShareSlice.span_config[span]
    step = span_config['slice']
    res = make_upper_lower(trim=span_config['slice'],
                           span=span_config['window'],
                           clip=span_config['slice'],
                           fmt="both")
    lower, upper, lower_stamp, upper_stamp = res

    workers = ShareSlice.get_span(user=address,
                                  share_type=share_types,
                                  algo=algos,
                                  lower=lower,
                                  upper=upper,
                                  stamp=True)

    highest_hash = 0
    for worker in workers:
        d = worker['data']
        d['label'] = "{} ({})".format(d['worker'] or "[unnamed]", d['algo'])
        hps = current_app.config['algos'][d['algo']]['hashes_per_share']
        for idx in worker['values']:
            worker['values'][idx] *= hps / step.total_seconds()
            if worker['values'][idx] > highest_hash:
                highest_hash = worker['values'][idx]

    scales = {1000: "KH/s", 1000000: "MH/s", 1000000000: "GH/s"}

    scale_label = "H/s"
    scale = 1
    for amnt, label in scales.iteritems():
        if amnt > scale and highest_hash > amnt:
            scale = amnt
            scale_label = label

    return jsonify(start=lower_stamp,
                   end=upper_stamp,
                   step=step.total_seconds(),
                   scale=scale,
                   scale_label=scale_label,
                   workers=workers)


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
        currency = addr[0]
        address = addr[1]

        try:
            curr = currencies.lookup_address(address)
        except AttributeError:
            return jsonify({currency: False})

        if currency == 'Any':
            return jsonify({curr.key: True})

        if not curr.key == currency:
            return jsonify({currency: False})
        else:
            return jsonify({currency: True})


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
                try:
                    currencies.lookup_address(address)
                except AttributeError:
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
    try:
        curr = currencies.lookup_address(address)
    except AttributeError:
        return render_template('invalid_address.html',
                               allowed_currencies=currencies.payout_currencies())

    result, alert_cls = handle_message(address, curr)

    user = UserSettings.query.filter_by(user=address).first()

    user_addresses = {}
    if not user:
        d_perc = 0
        anon = None
    else:
        anon = user.anon
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
    exchangeable_currencies.pop(curr.key)

    return render_template("user_settings.html",
                           username=address,
                           result=result,
                           alert_cls=alert_cls,
                           d_perc=d_perc,
                           user_currency=curr.name,
                           user_currency_name=curr.key,
                           anon=anon,
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
