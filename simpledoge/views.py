from flask import (render_template, Blueprint, jsonify, current_app, request,
                   abort)
from itsdangerous import TimedSerializer

from . import db
from .models import Transaction, CoinTransaction


main = Blueprint('main', __name__)


@main.route("/")
def home():
    return render_template('home.html')


@main.route("/get_transactions", methods=['POST'])
def get_transactions():
    """ Used by remote procedure call to retrieve a list of transactions to
    be processed. Transaction information is signed for safety. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    s.loads(request.data)

    struct = Transaction.serialize_pending()
    db.session.commit()
    return s.dumps(struct)


@main.route("/confirm_transactions", methods=['POST'])
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
        assert 'action' in data
        assert data['action'] in ['reset', 'confirm']
        if data['action'] == 'confirm':
            assert len(data['coin_txid']) == 64
        assert isinstance(data['txids'], list)
        for id in data['txids']:
            assert isinstance(id, int)
    except AssertionError:
        abort(400)

    if data['action'] == 'confirm':
        coin_trans = CoinTransaction.create(data['coin_txid'])
        vals = {Transaction.txid: coin_trans.txid}
        db.session.flush()
    else:
        vals = {Transaction.sent: False}
    Transaction.query.filter(Transaction.id.in_(data['txids'])).update(
        vals, synchronize_session=False)

    db.session.commit()

    return s.dumps(True)


@main.route("/nav_stats")
def nav_stats():
    es = Elasticsearch()
    res = es.search(index="p_stats", size="5", body={})

    nav_stats = [(r['_source']) for r in res['hits']['hits']]
    return jsonify(nav_stats=nav_stats)


@main.route("/pool_stats")
def pool_stats():

    es = Elasticsearch()
    res = es.search(index="p_hashrate", size="288", body={
        "query": {
            "match_all": {}
        },
        "sort": {
            "time": "desc"
        }

    })

    p_stats = [(list(r['_source'].values())) for r in res['hits']['hits']]
    return jsonify(points=p_stats, length=len(p_stats))


@main.route("/<address>")
def view_resume(address=None):
    return render_template('user_stats.html', username=address)


@main.route("/<address>/stats")
def address_stats(address=None):
    es = Elasticsearch()
    res = es.search(index="minute_shares", size="1440", fields="time,shares", body={
        "query": {
            "term": {
                'username':address
            }

        },
        "sort": {
            "time": "desc"
        }
    })
    min_shares = [(r['fields']['time'], r['fields']['shares']) for r in res['hits']['hits']]
    return jsonify(points=min_shares, length=len(min_shares))
