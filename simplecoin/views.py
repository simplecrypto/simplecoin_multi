import yaml
import datetime

from flask import (current_app, request, render_template, Blueprint, jsonify,
                   g, session, Response)

from .models import (Block, ShareSlice, UserSettings, make_upper_lower, Payout,
                     PayoutAggregate, DeviceSlice)
from . import db, root, cache, currencies, algos, locations
from .utils import (verify_message, collect_user_stats, get_pool_hashrate,
                    get_alerts, resort_recent_visit, collect_acct_items,
                    CommandException)


main = Blueprint('main', __name__)


@main.route("/")
def home():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    payout_currencies = currencies.exchangeable_currencies
    return render_template('home.html',
                           news=news,
                           payout_currencies=payout_currencies,
                           locations=locations)


@main.route("/configuration_guide")
def configuration_guide():
    return render_template('config_guide_wrapper.html',
                           locations=locations)


@main.route("/news")
def news():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    return render_template('news.html', news=news)


@main.route("/merge_blocks", defaults={"q": Block.merged == True})
@main.route("/blocks", defaults={"q": Block.merged == False})
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


@main.route("/leaderboard")
def leaderboard():
    # Holds a dictionary keyed by username
    users = {}
    # A dictionary for copying. The default value of each keyed user in the above
    algos_disp = [(a.display, a.key) for a in algos.active_algos()]
    algos_disp.append(('Normalized', 'normalized'))
    users = cache.get("leaderboard") or {}
    return render_template('leaderboard.html', users=users, algos=algos_disp)


@main.route("/<user_address>/account", defaults={'type': 'payout'})
@main.route("/<user_address>/aggr_account", defaults={'type': 'aggr'})
def account(user_address, type):
    page = int(request.args.get('page', 0))
    if page < 0:
        page = 0
    offset = page * 100

    if type == "aggr":
        aggrs = (PayoutAggregate.query.filter_by(user=user_address).join(Payout.block).
                 order_by(PayoutAggregate.created_at.desc()).limit(100).offset(offset))
        return render_template('account.html', aggregates=aggrs, page=page,
                               table="aggregate_table.html")
    else:
        payouts = (Payout.query.filter_by(user=user_address).join(Payout.block).
                   order_by(Block.found_at.desc()).limit(100).offset(offset))
        return render_template('account.html', payouts=payouts, page=page,
                               table="acct_table.html")


@main.route("/<address>/<worker>")
def worker_detail(address, worker):
    return render_template('worker_detail.html', username=address, worker=worker)


@main.route("/pool_stats")
def pool_stats():
    blocks_show = current_app.config.get('blocks_stats_page', 25)
    current_block = {'reward': cache.get('reward') or 0,
                     'difficulty': cache.get('difficulty') or 0,
                     'height': cache.get('blockheight') or 0}
    server_status = cache.get('server_status') or {}

    blocks = (db.session.query(Block).filter_by(merged=False).
              order_by(Block.found_at.desc()).limit(blocks_show))

    merge_blocks = (db.session.query(Block).filter_by(merged=True).
                    order_by(Block.found_at.desc()).limit(blocks_show))

    return render_template('pool_stats.html',
                           blocks=blocks,
                           merge_blocks=merge_blocks,
                           current_block=current_block,
                           server_status=server_status)


@main.before_request
def add_pool_stats():
    g.algos = {k: v for k, v in algos.iteritems() if v.enabled is True}
    g.hashrates = {a: get_pool_hashrate(a) for a in g.algos}
    # Dictionary keyed by algo
    g.miner_count = cache.get('total_miners') or {}
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


@main.route("/<user_address>")
def user_dashboard(user_address):
    # Do some checking to make sure the address is valid + payable
    try:
        currencies.lookup_payable_addr(user_address)
    except Exception:
        return render_template('invalid_address.html',
                                allowed_currencies=currencies.exchangeable_currencies)

    stats = collect_user_stats(user_address)

    # reorganize/create the recently viewed
    recent = session.get('recent_user_counts', {})
    recent.setdefault(user_address, 0)
    recent[user_address] += 1
    session['recent_user_counts'] = recent
    resort_recent_visit(recent)
    return render_template('user_stats.html',
                           username=user_address,
                           **stats)


@main.route("/<user_address>/clear")
def address_clear(user_address=None):
    # remove address from the recently viewed
    recent = session.get('recent_users_counts', {})
    try:
        del recent[user_address]
    except KeyError:
        pass
    resort_recent_visit(recent)

    return jsonify(recent=session['recent_users'])


@main.route("/api/<typ>")
def address_stats(typ):
    kwargs = {'worker': tuple()}
    kwargs['user'] = request.args['address'].split(",")
    if 'worker' in request.args:
        kwargs['worker'] = request.args["worker"].split(",")
    if typ == "devices":
        cls = DeviceSlice
        kwargs['stat_val'] = [DeviceSlice.to_db[request.args['stat']]]
    else:
        cls = ShareSlice
        algo = request.args.get('algos')
        kwargs['algo'] = tuple()
        if algo:
            kwargs['algo'] = algo.split(",")
        kwargs['share_type'] = request.args.get("share_types", "acc").split(",")

    span = int(request.args.get("span", 0))

    # store all the raw data of we've grabbed
    workers = {}
    span_config = ShareSlice.span_config[span]
    step = span_config['slice']
    res = make_upper_lower(trim=span_config['slice'],
                           span=span_config['window'],
                           clip=span_config['slice'],
                           fmt="both")
    lower, upper, lower_stamp, upper_stamp = res

    workers = cls.get_span(lower=lower,
                           slice_size=span,
                           upper=upper,
                           stamp=True,
                           **kwargs)

    highest_value = 0
    for worker in workers:
        d = worker['data']
        # Set the label for this data series
        if typ == "shares":
            d['label'] = "{} ({})".format(d['worker'] or "[unnamed]", d['algo'])
            hps = current_app.config['algos'][d['algo']]['hashes_per_share']
            for idx in worker['values']:
                worker['values'][idx] *= hps / step.total_seconds()
                if worker['values'][idx] > highest_value:
                    highest_value = worker['values'][idx]
        else:
            d['label'] = d['device']
            for idx in worker['values']:
                if worker['values'][idx] > highest_value:
                    highest_value = worker['values'][idx]

    if typ == "shares" or kwargs['stat_val'][0] == 0:
        scales = {1000: "KH/s", 1000000: "MH/s", 1000000000: "GH/s"}

        scale_label = "H/s"
        scale = 1
        for amnt, label in scales.iteritems():
            if amnt > scale and highest_value > amnt:
                scale = amnt
                scale_label = label
    else:
        scale_label = "Temperature"
        scale = 1

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
            if currency == 'Any':
                curr = currencies.lookup_payable_addr(address)
            else:
                curr = currencies.validate_bc_address(address)
        except ValueError:
            return jsonify({currency: False})

        if currency == 'Any':
            return jsonify({curr.key: True})

        if not currencies[currency] in curr:
            return jsonify({currency: False})
        else:
            return jsonify({currency: True})


@main.route("/settings/<user_address>", methods=['POST', 'GET'])
def settings(user_address):
    # Do some checking to make sure the address is valid + payable
    try:
        curr = currencies.lookup_payable_addr(user_address)
    except Exception:
        return render_template('invalid_address.html',
                                allowed_currencies=currencies.exchangeable_currencies)

    result, alert_cls = handle_message(user_address, curr)
    user = UserSettings.query.filter_by(user=user_address).first()
    return render_template("user_settings.html",
                           username=user_address,
                           result=result,
                           alert_cls=alert_cls,
                           user_currency=curr.name,
                           user_currency_name=curr.key,
                           user=user,
                           ex_currencies=currencies.exchangeable_currencies,
                           unex_currencies=currencies.unexchangeable_currencies)


@main.route("/crontabs")
def crontabs():
    stats = {}
    prefix = "cron_last_run_"
    prefix_len = len(prefix)
    for key in cache.cache._client.keys("{}*".format(prefix)):
        stats[key[prefix_len:]] = cache.cache._client.hgetall(key)

    return render_template("crontabs.html", stats=stats)
