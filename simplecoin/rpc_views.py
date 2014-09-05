import six
import sys

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


@rpc_views.route("/get_trade_requests", methods=['POST'])
def get_trade_requests():
    """ Used by remote procedure call to retrieve a list of sell requests to
    be processed. Transaction information is signed for safety. """
    current_app.logger.info("get_sell_requests being called, args of {}!".
                            format(g.signed))
    lock = False
    if isinstance(g.signed, dict) and g.signed['lock']:
        lock = True

    trade_requests = TradeRequest.query.filter_by(_status=0, locked=False).all()
    trs = [(tr.id, tr.currency, float(tr.quantity), tr.type) for tr in trade_requests]

    if lock:
        current_app.logger.info("Locking sell requests at retriever request.")
        for tr in trade_requests:
            tr.locked = True
        db.session.commit()
    return sign([trs, lock])


@rpc_views.route("/update_trade_requests", methods=['POST'])
def update_trade_requests():
    """ Used as a response from an rpc sell request system. This will update
    the amount received for a sell request and its status. Both request and
    response are signed. """
    # basic checking of input
    try:
        assert 'completed_trs' in g.signed
        assert isinstance(g.signed['completed_trs'], dict)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to update_sell_requests",
                                exc_info=True)
        abort(400)

    for tr_id, (quantity, fees) in g.signed['completed_trs'].iteritems():
        tr_id = int(tr_id)
        tr = (TradeRequest.query.filter_by(id=tr_id).with_lockmode('update').
              one())
        tr.exchanged_quantity = Decimal(quantity)
        tr.fees = Decimal(fees)
        tr._status = 6

        if not tr.payouts:
            current_app.logger.warn("Trade request has no attached payouts")
        if tr.payouts:
            # calculate user payouts based on percentage of the total
            # exchanged value
            for payout in tr.payouts:
                if tr.type == "sell":
                    assert payout.sell_amount is None
                    current_app.logger.warn(tr.exchanged_quantity)
                    payout.sell_amount = (payout.amount * tr.exchanged_quantity) / tr.quantity
                elif tr.type == "buy":
                    assert payout.buy_amount is None
                    payout.buy_amount = (payout.sell_amount * tr.exchanged_quantity) / tr.quantity
                    payout.payable = True
                else:
                    raise AttributeError("Invalid tr type")

            # double check successful distribution at the satoshi
            if tr.type == "sell":
                payout_amt = sum([p.sell_amount for p in tr.payouts]).\
                    quantize(current_app.SATOSHI)
                assert payout_amt == tr.exchanged_quantity.quantize(current_app.SATOSHI)
            elif tr.type == "buy":
                buy_amount = sum([p.buy_amount for p in tr.payouts]).\
                    quantize(current_app.SATOSHI)
                assert buy_amount == tr.exchanged_quantity.quantize(current_app.SATOSHI)

            current_app.logger.info(
                "Successfully pushed trade result for request id {:,} and "
                "amount {:,} to {:,} payouts.".
                format(tr.id, tr.exchanged_quantity, len(tr.payouts)))

    db.session.commit()
    return sign(dict(success=True, result="Trade requests successfully updated."))


@rpc_views.route("/reset_trade_requests", methods=['POST'])
def reset_trade_requests():
    """ Used as a response from an rpc sell request system. This will reset
    the locked status of a list of sell requests upon failure on the remote
    side. Both request and response are signed. """
    # basic checking of input
    try:
        assert 'reset' in g.signed
        assert isinstance(g.signed['reset'], bool)
        assert isinstance(g.signed['tr_ids'], list)
        for id in g.signed['tr_ids']:
            assert isinstance(id, int)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to reset_trade_requests",
                                exc_info=True)
        abort(400)

    if g.signed['reset'] and g.signed['tr_ids']:
        srs = TradeRequest.query.filter(TradeRequest.id.in_(g.signed['tr_ids'])).all()
        for sr in srs:
            sr.locked = False
        db.session.commit()
        return sign(dict(success=True, result="Successfully reset"))

    return sign(dict(result=True))


@rpc_views.route("/get_payouts", methods=['POST'])
def get_payouts():
    """ Used by remote procedure call to retrieve a list of transactions to
    be processed. Transaction information is signed for safety. """
    current_app.logger.info("get_payouts being called, args of {}!".format(g.signed))
    merged = None
    if isinstance(g.signed, dict) and g.signed['merged']:
        merged = g.signed['merged']

    with Benchmark("Fetching payout information"):
        query = (Payout.query.filter_by(transaction_id=None, merged_type=merged).
                 join(Payout.block, aliased=True).filter_by(mature=True))
        pids = [(p.user, p.amount, p.id) for p in query]
    return sign(dict(pids=pids))


@rpc_views.route("/update_payouts", methods=['POST'])
def update_transactions():
    """ Used as a response from an rpc payout system. This will either reset
    the locked status of a list of transactions upon failure on the remote
    side, or create a new CoinTransaction object and link it to the
    transactions to signify that the transaction has been processed. Both
    request and response are signed. """
    # basic checking of input
    try:
        if 'coin_txid' in g.signed:
            assert len(g.signed['coin_txid']) == 64
        else:
            assert 'reset' in g.signed
            assert isinstance(g.signed['reset'], bool)
        assert isinstance(g.signed['pids'], list)
        assert isinstance(g.signed['bids'], list)
        for id in g.signed['pids']:
            assert isinstance(id, int)
        for id in g.signed['bids']:
            assert isinstance(id, int)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to confirm",
                                exc_info=True)
        abort(400)

    return sign(dict(result=True))


@rpc_views.route("/confirm_transactions", methods=['POST'])
def confirm_transactions():
    """ Used to confirm that a transaction is now complete on the network. """
    # basic checking of input
    try:
        assert isinstance(g.signed['tids'], list)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to confirm_transactions",
                                exc_info=True)
        abort(400)

    Transaction.query.filter(Transaction.txid.in_(g.signed['tids'])).update(
        {Transaction.confirmed: True}, synchronize_session=False)
    db.session.commit()

    return sign(dict(result=True))
