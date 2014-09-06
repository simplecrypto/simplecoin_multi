import six
import sys
import sqlalchemy

from flask import current_app, request, abort, Blueprint, g
from itsdangerous import TimedSerializer, BadData
from decimal import Decimal

from .models import Transaction, PayoutAggregate, TradeRequest
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
    trade_requests = TradeRequest.query.filter_by(_status=0).all()
    trs = [(tr.id, tr.currency, float(tr.quantity), tr.type) for tr in trade_requests]
    return sign(dict(trs=trs))


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
            current_app.logger.warn("Trade request #{} has no attached payouts".format(tr.id))
        if tr.payouts:
            # calculate user payouts based on percentage of the total
            # exchanged value
            for payout in tr.payouts:
                if tr.type == "sell":
                    assert payout.sell_amount is None
                    payout.sell_amount = (payout.amount * tr.exchanged_quantity) / tr.quantity
                elif tr.type == "buy":
                    assert payout.buy_amount is None
                    payout.buy_amount = (payout.sell_amount * tr.exchanged_quantity) / tr.quantity
                    payout.payable = True
                else:
                    raise AttributeError("Invalid tr type \'{}\'".format(tr.type))

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
    return sign(dict(success=True,
                     result="Trade requests successfully updated."))


@rpc_views.route("/get_payouts", methods=['POST'])
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
        query = PayoutAggregate.query.filter_by(transaction_id=None,
                                                currency=currency)
        # XXX: Add the min payout amount code here!
        pids = [(p.user, str(p.amount), p.id) for p in query]
    return sign(dict(pids=pids))


@rpc_views.route("/associate_payouts", methods=['POST'])
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
            assert isinstance(id, int)
        tx_fee = Decimal(g.signed['tx_fee'])
        currency = g.signed['currency']
    except (AssertionError, KeyError, TypeError):
        current_app.logger.warn("Invalid data passed to confirm",
                                exc_info=True)
        abort(400)

    with Benchmark("Associating payout transaction ids"):
        try:
            trans = Transaction(txid=g.signed['coin_txid'],
                                fees=tx_fee,
                                currency=currency)
            db.session.add(trans)
            db.session.flush()
        except sqlalchemy.exc.IntegrityError:
            db.session.rollback()
            current_app.logger.warn("Transaction id {} already exists!"
                                    .format(g.signed['coin_txid']))

        PayoutAggregate.query.filter(
            PayoutAggregate.id.in_(g.signed['pids'])).update(
                {PayoutAggregate.transaction_id: g.signed['coin_txid']},
                synchronize_session=False)

        db.session.commit()

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

    txdata = {}
    for txid in g.signed['tids']:
        txdata.setdefault(txid, {})
        txdata[txid][Transaction.confirmed] = True

    for txid in txdata:
        Transaction.query.filter(Transaction.txid.in_(txid)).update(
            txdata[txid], synchronize_session=False)
    db.session.commit()

    return sign(dict(result=True))
