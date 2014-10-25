from __future__ import division
import yaml

from flask import (current_app, request, render_template, Blueprint, jsonify,
                   g, session, Response, abort)

from .models import (Block, ShareSlice, UserSettings, make_upper_lower, Credit,
                     Payout, DeviceSlice, Transaction)
from . import db, root, cache, currencies, algos, locations
from .exceptions import InvalidAddressException
from .utils import (verify_message, collect_user_stats, get_pool_hashrate,
                    get_alerts, resort_recent_visit, collect_acct_items,
                    CommandException, anon_users, collect_pool_stats)


main = Blueprint('main', __name__)


@main.route("/")
def home():
    payout_currencies = currencies.buyable_currencies
    return render_template('home.html',
                           payout_currencies=payout_currencies,
                           locations=locations)


@main.route("/configuration_guide")
def configuration_guide():
    payout_currencies = currencies.buyable_currencies
    return render_template('config_guide_wrapper.html',
                           payout_currencies=payout_currencies,
                           locations=locations)


@main.route("/faq")
def faq():
    faq = yaml.load(open(root + '/static/yaml/faq.yaml'))
    return render_template('faq.html', faq=faq)


@main.route("/news")
def news():
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    return render_template('news.html', news=news)


@main.route("/merge_blocks", defaults={"q": Block.merged == True})
@main.route("/blocks", defaults={"q": Block.merged == False})
@main.route("/blocks/<currency>")
def blocks(q=None, currency=None):
    page = int(request.args.get('page', 0))
    if page < 0:
        page = 0
    offset = page * 100
    blocks = Block.query.order_by(Block.found_at.desc())
    if q is not None:
        blocks = blocks.filter(q)
    if currency:
        blocks = blocks.filter_by(currency=currency)

    blocks = blocks.offset(offset).limit(100)
    return render_template('blocks.html', blocks=blocks, page=page)


@main.route("/networks")
def networks():
    """ A page to display current information about each of the networks we are
    mining on.  """
    network_data = {}
    for currency in currencies.itervalues():
        data = cache.get("{}_data".format(currency.key))
        if data:
            network_data[currency] = data
    return render_template('networks.html', network_data=network_data)


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
@main.route("/<user_address>/aggr_account", defaults={'type': 'credit'})
def account(user_address, type):
    page = int(request.args.get('page', 0))
    if page < 0:
        page = 0
    offset = page * 100

    if type == "payout":
        payouts = (Payout.query.filter_by(user=user_address).
                   order_by(Payout.created_at.desc()).limit(100).offset(offset))
        return render_template('account.html', payouts=payouts, page=page,
                               table="payout_table.html")
    else:
        credits = (Credit.query.filter_by(user=user_address).join(Credit.block).
                   order_by(Block.found_at.desc()).limit(100).offset(offset))
        return render_template('account.html', credits=credits, page=page,
                               table="credit_table.html")


@main.route("/transaction/<txid>")
def transaction_detail(txid):
    tx = Transaction.query.filter_by(txid=txid).first()
    return render_template('transaction_details.html', tx=tx)


@main.route("/block/<blockhash>")
def block_detail(blockhash):
    block = db.session.query(Block).filter_by(hash=blockhash).first()
    return render_template('block_details.html', block=block)


@main.route("/<address>/<worker>")
def worker_detail(address, worker):
    return render_template('worker_detail.html', username=address, worker=worker)


@main.route("/pool_stats")
def pool_stats():
    pool_stats = collect_pool_stats()
    return render_template('pool_stats.html', **pool_stats)


@main.before_request
def add_pool_stats():
    g.algos = {k: v for k, v in algos.iteritems() if v.enabled is True}
    g.hashrates = {a: get_pool_hashrate(a) for a in g.algos}
    # Dictionary keyed by algo
    g.miner_count = cache.get('total_miners') or {}
    g.alerts = get_alerts()
    g.anon_users = anon_users()


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


@main.route("/stats/<user_address>")
def user_dashboard(user_address):
    # Do some checking to make sure the address is valid + payable
    try:
        currencies.lookup_payable_addr(user_address)
    except Exception:
        return render_template(
            'invalid_address.html',
            allowed_currencies=currencies.buyable_currencies)

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
                           clip=span_config['slice'] * 2,
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
    return render_template("500.html", no_header=True)


def handle_message(address, curr):
    alert_cls = "danger"
    result = None
    vals = request.form
    if request.method == "POST":
        try:
            verify_message(address, curr, vals['message'], vals['signature'])
        except CommandException as e:
            result = "Error: {}".format(e)
            # lets just log all errors people are getting
            current_app.logger.info(
                "Command exception in Command validation",
                exc_info=True)
        except Exception as e:
            current_app.logger.info(
                "Unhandled exception in Command validation",
                exc_info=True)
            result = "An unhandled error occurred: {}".format(e)
        else:
            result = "Successfully changed!"
            alert_cls = "success"

    return result, alert_cls


@main.route("/validate_address", methods=['POST'])
def validate_address():
    """ An endpoint that allows us to validate that addresses meet different
    types of requirements.

    Input is a json dictionary with:
        currency: the three letter code, or the string 'Any'
        address: the address string
        type: the type of address it must be

    Return value is like {'LTC': True} where LTC is currency provided
    """
    def validate(address, typ, currency):
        try:
            ver = currencies.validate_bc_address(address)
        except InvalidAddressException:
            return False

        if typ == 'buyable':
            lst = currencies.buyable_currencies
        elif typ == 'sellable':
            lst = currencies.sellable_currencies
        elif typ == 'unsellable':
            lst = currencies.unsellable_currencies
        elif typ == 'unbuyable':
            lst = currencies.unbuyable_currencies
        else:
            abort(400)

        for curr in lst:
            if ver in curr.address_version:
                if curr.key == currency or currency == 'Any':
                    return True
        return False

    data = request.json
    if validate(data['address'], data['type'], data['currency']):
        return jsonify({data['currency']: True})
    else:
        return jsonify({data['currency']: False})


@main.route("/settings/<user_address>", methods=['POST', 'GET'])
def settings(user_address):
    # Do some checking to make sure the address is valid + payable
    try:
        curr = currencies.lookup_payable_addr(user_address)
    except Exception:
        return render_template(
            'invalid_address.html',
            allowed_currencies=currencies.buyable_currencies)

    result, alert_cls = handle_message(user_address, curr)
    user = UserSettings.query.filter_by(user=user_address).first()
    return render_template("user_settings.html",
                           username=user_address,
                           result=result,
                           alert_cls=alert_cls,
                           user_currency=curr.name,
                           user_currency_name=curr.key,
                           user=user,
                           ex_currencies=currencies.sellable_currencies,
                           unex_currencies=currencies.unsellable_currencies)


@main.route("/crontabs")
def crontabs():
    stats = {}
    prefix = "cron_last_run_"
    prefix_len = len(prefix)
    for key in cache.cache._client.keys("{}*".format(prefix)):
        stats[key[prefix_len:]] = cache.cache._client.hgetall(key)

    return render_template("crontabs.html", stats=stats)
