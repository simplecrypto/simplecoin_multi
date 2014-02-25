import time
import yaml
import datetime
from itsdangerous import TimedSerializer
from flask import (current_app, request, render_template, Blueprint, abort,
                   jsonify, g)
from sqlalchemy.sql import func

from .models import Transaction, OneMinuteShare, Block, Share, Payout, last_block_share_id, last_block_time
from . import db, root, cache


main = Blueprint('main', __name__)


@main.route("/")
def home():
    blocks = db.session.query(Block).limit(10)
    news = yaml.load(open(root + '/static/yaml/news.yaml'))
    alerts = yaml.load(open(root + '/static/yaml/alerts.yaml'))
    return render_template('home.html', news=news, alerts=alerts, blocks=blocks)


@main.route("/get_payouts", methods=['POST'])
def get_payouts():
    """ Used by remote procedure call to retrieve a list of transactions to
    be processed. Transaction information is signed for safety. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    s.loads(request.data)

    struct = [(p.user, p.amount, p.id)
              for p in Payout.query.filter_by(transaction_id=None)]
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


@cache.cached(timeout=60, key_prefix='get_total_n1')
def get_frontpage_data():
    # A bit inefficient, but oh well... Make it better later...
    last_share_id = last_block_share_id()
    last_found_at = last_block_time()
    shares = db.session.query(func.sum(Share.shares)).filter(Share.id > last_share_id).scalar() or 0
    last_dt = (datetime.datetime.utcnow() - last_found_at).total_seconds()
    return shares, last_dt


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
    payouts = db.session.query(Payout).filter_by(user=address).limit(20)
    block = Block.query.order_by(Block.height.desc()).first()
    user_shares = db.session.query(func.sum(Share.shares))\
        .filter_by(user=address)\
        .filter(Share.id > block.last_share_id)\
        .scalar()
    current_difficulty = 1057
    return render_template('user_stats.html',
                           username=address,
                           user_shares=user_shares,
                           current_difficulty=current_difficulty,
                           payouts=payouts)


@main.route("/<address>/stats")
def address_stats(address=None):
    minutes = db.session.query(OneMinuteShare).filter_by(user=address)
    data = {time.mktime(minute.minute.utctimetuple()): minute.shares
            for minute in minutes}
    day_ago = ((int(time.time()) - (60 * 60 * 24)) // 60) * 60
    out = [(i, data.get(i) or 0)
           for i in xrange(day_ago, day_ago + (1440 * 60), 60)]

    return jsonify(points=out, length=len(out))
