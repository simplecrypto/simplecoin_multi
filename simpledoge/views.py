import calendar
import json
import time
import yaml
import datetime

from itsdangerous import TimedSerializer
from flask import (current_app, request, render_template, Blueprint, abort,
                   jsonify, g, session)
from sqlalchemy.sql import func

from .models import (Transaction, OneMinuteShare, Block, Share, Payout,
                     last_block_share_id, last_block_time, Blob, FiveMinuteShare,
                     OneHourShare, Status)
from . import db, root, cache


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
    return render_template('pool_stats.html', blocks=blocks,
                           current_block=current_block)


@main.route("/get_payouts", methods=['POST'])
def get_payouts():
    """ Used by remote procedure call to retrieve a list of transactions to
    be processed. Transaction information is signed for safety. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    s.loads(request.data)

    payouts = (Payout.query.filter_by(transaction_id=None).
               join(Payout.transaction, aliased=True).filter_by(confirmed=True))
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
    ratio = g.pool_stats[0]/g.pool_stats[1]
    additional_shares = ratio * additional_seconds
    g.pool_stats[0] += additional_shares
    g.pool_stats[1] += additional_seconds
    g.current_difficulty = db.session.query(Blob).filter_by(key="block").first().data['difficulty']

    alerts = yaml.load(open(root + '/static/yaml/alerts.yaml'))
    g.alerts = alerts


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
    ten_ago = (datetime.datetime.utcnow() - datetime.timedelta(minutes=10)).replace(second=0)
    minutes = (OneMinuteShare.query.
               filter_by(user=user).filter(OneMinuteShare.time >= ten_ago).
               order_by(OneMinuteShare.time.desc()).
               limit(10))
    if minutes:
        return sum([min.value for min in minutes])
    return 0


@cache.memoize(timeout=60)
def total_earned(user):
    return (db.session.query(func.sum(Payout.amount)).
            filter_by(user=user).scalar() or 0)

@cache.memoize(timeout=60)
def total_paid(user):
    total_p = (Payout.query.filter_by(user=user).
              join(Payout.transaction, aliased=True).
              filter_by(confirmed=True))
    return sum([tx.amount for tx in total_p])

@cache.memoize(timeout=600)
def user_summary():
    """ Returns all users that contributed shares to the last round """
    return (db.session.query(func.sum(Share.shares), Share.user).
            group_by(Share.user).filter(Share.id > last_block_share_id()).all())


@main.route("/share_summary")
def summary_page():
    users = user_summary()
    users = sorted(users, key=lambda x: x[0], reverse=True)
    return render_template('share_summary.html', users=users)


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
    earned = total_earned(address)
    total_paid = (Payout.query.filter_by(user=address).
                  join(Payout.transaction, aliased=True).
                  filter_by(confirmed=True))
    total_paid = sum([tx.amount for tx in total_paid])
    balance = earned - total_paid
    unconfirmed_balance = (Payout.query.filter_by(user=address).
                           join(Payout.block, aliased=True).
                           filter_by(mature=False))
    unconfirmed_balance = sum([payout.amount for payout in unconfirmed_balance])
    balance -= unconfirmed_balance

    payouts = db.session.query(Payout).filter_by(user=address).limit(20)
    last_share_id = last_block_share_id()
    user_shares = (db.session.query(func.sum(Share.shares)).
                   filter(Share.id > last_share_id, Share.user == address).
                   scalar() or 0)

    statuses = Status.query.filter_by(user=address).order_by(Status.worker.asc()).all()

    # reorganize/create the recently viewed
    recent = session.get('recent_users', [])
    if address in recent:
        recent.remove(address)
    recent.insert(0, address)
    session['recent_users'] = recent[:10]

    return render_template('user_stats.html',
                           username=address,
                           statuses=statuses,
                           user_shares=user_shares,
                           payouts=payouts,
                           round_reward=250000,
                           total_earned=earned,
                           total_paid=total_paid,
                           balance=balance,
                           unconfirmed_balance=unconfirmed_balance)


@main.route("/<address>/stats")
@main.route("/<address>/stats/<window>")
def address_stats(address=None, window="hour"):
    # store all the raw data of we've grabbed
    workers = {}

    def get_typ(typ, window=True):
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

    def compress_typ(typ):
        for slc in get_typ(typ, window=False):
            slice_dt = typ.upper.floor_time(slc.time)
            stamp = calendar.timegm(slice_dt.utctimetuple())
            workers.setdefault(slc.worker, {})
            workers[slc.worker].setdefault(stamp, 0)
            workers[slc.worker][stamp] += slc.value

    if window == "hour":
        typ = OneMinuteShare
    elif window == "day":
        compress_typ(OneMinuteShare)
        typ = FiveMinuteShare
    elif window == "month":
        compress_typ(FiveMinuteShare)
        typ = OneHourShare

    for m in get_typ(typ):
        stamp = calendar.timegm(m.time.utctimetuple())
        workers.setdefault(m.worker, {})
        workers[m.worker][stamp] = m.value
    step = typ.slice_seconds
    end = ((int(time.time()) // step) * step) - (step * 2)
    start = end - typ.window.total_seconds() + (step * 2)

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
