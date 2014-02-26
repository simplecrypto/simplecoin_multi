import calendar
import time
import yaml
import datetime
from itsdangerous import TimedSerializer
from flask import (current_app, request, render_template, Blueprint, abort,
                   jsonify, g)
from sqlalchemy.sql import func

from .models import Transaction, OneMinuteShare, Block, Share, Payout, last_block_share_id, last_block_time, Blob
from . import db, root, cache


main = Blueprint('main', __name__)


@main.route("/")
def home():
    current_block = db.session.query(Blob).filter_by(key="block").first()
    blocks = db.session.query(Block).order_by(Block.height.desc()).limit(10)
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    alerts = yaml.load(open(root + '/static/yaml/alerts.yaml'))
    return render_template('home.html', news=news, alerts=alerts, blocks=blocks,
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


@cache.cached(timeout=60, key_prefix='get_total_n1')
def get_frontpage_data():

    # A bit inefficient, but oh well... Make it better later...
    last_share_id = last_block_share_id()
    last_found_at = last_block_time()
    dt = datetime.datetime.utcnow()
    five_min = (OneMinuteShare.query.filter_by(user='pool')
                .order_by(OneMinuteShare.minute.desc())
                .limit(5))
    five_min = sum([min.shares for min in five_min])
    shares = db.session.query(func.sum(Share.shares)).filter(Share.id > last_share_id).scalar() or 0
    last_dt = (datetime.datetime.utcnow() - last_found_at).total_seconds()
    return [shares, last_dt, dt, five_min]


@main.route("/pool_stats")
def pool_stats():
    minutes = db.session.query(OneMinuteShare).filter_by(user="pool")
    data = {time.mktime(minute.minute.utctimetuple()): minute.shares
            for minute in minutes}
    day_ago = ((int(time.time()) - (60 * 60 * 24)) // 60) * 60
    out = [(i, data.get(i) or 0)
           for i in xrange(day_ago, day_ago + (1440 * 60), 60)]

    return jsonify(points=out, length=len(out))


@main.route("/<address>")
def user_dashboard(address=None):
    total_earned = (db.session.query(func.sum(Payout.amount)).
                    filter_by(user=address).scalar() or 0)
    total_paid = (Payout.query.filter_by(user=address).
                  join(Payout.transaction, aliased=True).
                  filter_by(confirmed=True))
    total_paid = sum([tx.amount for tx in total_paid])
    balance = total_earned - total_paid
    unconfirmed_balance = (Payout.query.filter_by(user=address).
                           join(Payout.block, aliased=True).
                           filter_by(mature=False))
    unconfirmed_balance = sum([payout.amount for payout in unconfirmed_balance])
    balance -= unconfirmed_balance

    payouts = db.session.query(Payout).filter_by(user=address).limit(20)
    last_share_id = last_block_share_id()
    user_shares = db.session.query(func.sum(Share.shares)).filter(Share.id > last_share_id, Share.user == address).scalar() or 0

    current_difficulty = db.session.query(Blob).filter_by(key="block").first().data['difficulty']
    return render_template('user_stats.html',
                           username=address,
                           user_shares=user_shares,
                           current_difficulty=current_difficulty,
                           payouts=payouts,
                           round_reward=250000,
                           total_earned=total_earned,
                           total_paid=total_paid,
                           balance=balance,
                           unconfirmed_balance=unconfirmed_balance)


@main.route("/<address>/stats")
def address_stats(address=None):
    minutes = db.session.query(OneMinuteShare).filter_by(user=address)
    data = {calendar.timegm(minute.minute.utctimetuple()): minute.shares
            for minute in minutes}
    day_ago = ((int(time.time()) - (60 * 60 * 24)) // 60) * 60
    out = [(i, data.get(i) or 0)
           for i in xrange(day_ago, day_ago + (1440 * 60), 60)]

    return jsonify(points=out, length=len(out))
