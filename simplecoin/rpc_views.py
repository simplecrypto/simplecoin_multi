import six
import sys
import sqlalchemy

from flask import current_app, request, abort, Blueprint, g
from itsdangerous import TimedSerializer, BadData
from decimal import Decimal

from .models import Transaction, Payout, TradeRequest
from .utils import Benchmark
from . import db


rpc_views = Blueprint('rpc_views', __name__)


@rpc_views.errorhandler(Exception)
def api_error_handler(exc):
    try:
        six.reraise(type(exc), exc, tb=sys.exc_info()[2])
    except Exception:
        current_app.logger.error(
            "Unhandled exception encountered in rpc view",
            exc_info=True)
    resp = dict(result=False)
    return sign(resp, 500)


def sign(data, code=200):
    serialized = g.signer.dumps(data)
    return serialized


@rpc_views.before_request
def check_signature():
    g.signer = TimedSerializer(current_app.config['rpc_signature'])
    try:
        g.signed = g.signer.loads(request.data)
    except BadData:
        abort(403)


@rpc_views.route("/rpc/get_trade_requests", methods=['POST'])
def get_trade_requests():
    """ Used by remote procedure call to retrieve a list of sell requests to
    be processed. Transaction information is signed for safety. """
    current_app.logger.info("get_sell_requests being called, args of {}!".
                            format(g.signed))
    trade_requests = TradeRequest.query.filter_by(_status=0).all()
    trs = [(tr.id, tr.currency, float(tr.quantity), tr.type) for tr in trade_requests]
    return sign(dict(trs=trs))


@rpc_views.route("/rpc/update_trade_requests", methods=['POST'])
def update_trade_requests():
    """ Used as a response from an rpc sell request system. This will update
    the amount received for a sell request and its status. Both request and
    response are signed. """
    # basic checking of input
    try:
        assert 'trs' in g.signed
        assert isinstance(g.signed['trs'], dict)
        for tr_id, tr in g.signed['trs'].iteritems():
            assert isinstance(tr_id, int)
            assert isinstance(tr, dict)
            assert 'status' in tr
            assert isinstance(tr['status'], int)
            if tr['status'] == 6:
                assert 'quantity' in tr
                assert 'fees' in tr
    except AssertionError:
        current_app.logger.warn("Invalid data passed to update_sell_requests",
                                exc_info=True)
        abort(400)

    updated = []
    for tr_id, tr_dict in g.signed['trs'].iteritems():
        try:
            tr = (TradeRequest.query.filter_by(id=int(tr_id)).
                  with_lockmode('update').one())
            tr._status = tr_dict['status']

            if tr_dict['status'] == 6:
                tr.exchanged_quantity = Decimal(tr_dict['quantity'])
                tr.fees = Decimal(tr_dict['fees'])
                tr.distribute()
        except Exception:
            db.session.rollback()
            current_app.logger.error("Unable to update trade request {}"
                                     .format(tr_id), exc_info=True)
            abort(400)
        else:
            updated.append(tr_id)

    db.session.commit()
    return sign(dict(success=True, updated_ids=updated,
                     result="Trade requests successfully updated."))


@rpc_views.route("/rpc/get_payouts", methods=['POST'])
def get_payouts():
    """ Used by remote procedure call to retrieve a list of payout amounts to
    be processed. Transaction information is signed for safety. """
    current_app.logger.info("get_payouts being called, args of {}!"
                            .format(g.signed))
    try:
        currency = g.signed['currency']
    except KeyError:
        current_app.logger.warn("Invalid data passed to get_payouts",
                                exc_info=True)
        abort(400)

    with Benchmark("Fetching payout information"):
        query = Payout.query.filter_by(transaction_id=None, currency=currency)
        pids = [(p.user, p.address, str(p.amount), p.id) for p in query]
    return sign(dict(pids=pids))


@rpc_views.route("/rpc/associate_payouts", methods=['POST'])
def associate_payouts():
    """ Used to update a SC Payout with a network transaction. This will
    create a new CoinTransaction object and link it to the
    transactions to signify that the transaction has been processed. """
    # basic checking of input
    try:
        assert 'coin_txid' in g.signed
        assert 'pids' in g.signed
        assert len(g.signed['coin_txid']) == 64
        assert isinstance(g.signed['pids'], list)

        for id in g.signed['pids']:
            id = int(id)
        tx_fee = Decimal(g.signed['tx_fee'])
        currency = g.signed['currency']
    except (AssertionError, KeyError, TypeError):
        current_app.logger.warn("Invalid data passed to confirm",
                                exc_info=True)
        abort(400)

    with Benchmark("Associating payout transaction ids"):

        trans = (db.session.query(Transaction)
                 .filter_by(txid=g.signed['coin_txid'], currency=currency)
                 .first())
        if not trans:
            try:
                trans = Transaction(txid=g.signed['coin_txid'],
                                    network_fee=tx_fee,
                                    currency=currency)
                db.session.add(trans)
                db.session.flush()
            except sqlalchemy.exc.SQLAlchemyError as e:
                db.session.rollback()
                current_app.logger.warn("Got an SQLalchemy error attempting to "
                                        "create a new transaction with id {}. "
                                        "\nError:"
                                        .format(g.signed['coin_txid'], e))
                abort(400)

        Payout.query.filter(
            Payout.id.in_(g.signed['pids'])).update(
                {Payout.transaction_id: trans.id},
                synchronize_session=False)

        db.session.commit()

    return sign(dict(result=True))


@rpc_views.route("/rpc/confirm_transactions", methods=['POST'])
def confirm_transactions():
    """ Used to confirm that a transaction is now complete on the network. """
    # basic checking of input
    try:
        assert isinstance(g.signed['tids'], list)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to confirm_transactions",
                                exc_info=True)
        abort(400)

    transactions = (db.session.query(Transaction)
                    .filter(Transaction.txid.in_(g.signed['tids']))
                    .all())

    for transaction in transactions:
        transaction.confirmed = True

    db.session.commit()

    return sign(dict(result=True))
