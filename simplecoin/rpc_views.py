from itsdangerous import TimedSerializer
from flask import current_app, request, abort, Blueprint

from .models import Transaction, Payout, TradeRequest
from .utils import Benchmark
from . import db


main = Blueprint('rpc', __name__)


@main.route("/get_trade_requests", methods=['POST'])
def get_trade_requests():
    """ Used by remote procedure call to retrieve a list of sell requests to
    be processed. Transaction information is signed for safety. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    args = s.loads(request.data)
    current_app.logger.info("get_sell_requests being called, args of {}!".
                            format(args))
    lock = False
    if isinstance(args, dict) and args['lock']:
        lock = True

    trade_requests = TradeRequest.query.filter_by(_status=0, locked=False).all()
    trs = [(tr.id, tr.currency, tr.quantity, tr.type) for tr in trade_requests]

    if lock:
        current_app.logger.info("Locking sell requests at retriever request.")
        for tr in trade_requests:
            tr.locked = True
        db.session.commit()
    return s.dumps([trs, lock])


@main.route("/update_trade_requests", methods=['POST'])
def update_trade_requests():
    """ Used as a response from an rpc sell request system. This will update
    the amount received for a sell request and its status. Both request and
    response are signed. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    data = s.loads(request.data)

    # basic checking of input
    try:
        assert 'completed_trs' in data
        assert isinstance(data['completed_trs'], dict)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to update_sell_requests",
                                exc_info=True)
        abort(400)

    for tr_id, (quantity, fees) in data['completed_trs'].iteritems():
        tr_id = int(tr_id)
        tr = (TradeRequest.query.filter_by(id=tr_id).with_lockmode('update').one())
        tr.exchanged_quantity = quantity
        tr.fees = fees
        tr._status = 6

        if not tr.payouts:
            raise Exception("Trade request has no attached payouts")
        if tr.payouts:
            # truncated delivery based on the payouts percentage of the total
            # exchanged value
            distributed = 0
            for payout in tr.payouts:
                if tr.type == "sell":
                    assert payout.sell_amount is None
                    payout.sell_amount = float(
                        payout.amount * tr.exchanged_quantity) // tr.quantity
                    distributed += payout.sell_amount
                elif tr.type == "buy":
                    assert payout.buy_amount is None
                    payout.buy_amount = float(
                        payout.sell_amount * tr.exchanged_quantity) // tr.quantity
                    distributed += payout.buy_amount
                    payout.payable = True
                else:
                    raise AttributeError("Invalid tr type")

            current_app.logger.info(
                "Total accrued after trunated iteration {:,}; {}%"
                .format(distributed / 100000000.0, (distributed / float(tr.exchanged_quantity)) * 100))

            # round robin distribution of the remaining fractional satoshi
            i = 0
            while distributed < tr.exchanged_quantity:
                for payout in tr.payouts:
                    if tr.type == "sell":
                        payout.sell_amount += 1
                    elif tr.type == "buy":
                        payout.buy_amount += 1
                    distributed += 1
                    i += 1

                    # exit if we've exhausted
                    if distributed >= tr.exchanged_quantity:
                        break

            current_app.logger.info("Ran round robin distro {} times to finish "
                                    "distrib".format(i))

            # double check complete and exact distribution
            if tr.type == "sell":
                assert sum([p.sell_amount for p in tr.payouts]) == tr.exchanged_quantity
            elif tr.type == "buy":
                assert sum([p.buy_amount for p in tr.payouts]) == tr.exchanged_quantity

            current_app.logger.info(
                "Successfully pushed trade result for request id {:,} and amount {:,} to {:,} payouts."
                .format(tr.id, tr.exchanged_quantity / 100000000.0, len(tr.payouts)))

    db.session.commit()
    return s.dumps(dict(success=True, result="Trade requests successfully updated."))


@main.route("/reset_trade_requests", methods=['POST'])
def reset_trade_requests():
    """ Used as a response from an rpc sell request system. This will reset
    the locked status of a list of sell requests upon failure on the remote
    side. Both request and response are signed. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    data = s.loads(request.data)

    # basic checking of input
    try:
        assert 'reset' in data
        assert isinstance(data['reset'], bool)
        assert isinstance(data['tr_ids'], list)
        for id in data['tr_ids']:
            assert isinstance(id, int)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to reset_trade_requests",
                                exc_info=True)
        abort(400)

    if data['reset'] and data['tr_ids']:
        srs = TradeRequest.query.filter(TradeRequest.id.in_(data['tr_ids'])).all()
        for sr in srs:
            sr.locked = False
        db.session.commit()
        return s.dumps(dict(success=True, result="Successfully reset"))

    return s.dumps(True)


@main.route("/get_payouts", methods=['POST'])
def get_payouts():
    """ Used by remote procedure call to retrieve a list of transactions to
    be processed. Transaction information is signed for safety. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    args = s.loads(request.data)
    current_app.logger.info("get_payouts being called, args of {}!".format(args))
    lock = False
    merged = None
    if isinstance(args, dict) and args['lock']:
        lock = True
    if isinstance(args, dict) and args['merged']:
        merged = args['merged']

    with Benchmark("Fetching payout information"):
        pids = [(p.user, p.amount, p.id) for p in Payout.query.filter_by(transaction_id=None, locked=False, merged_type=merged).
                join(Payout.block, aliased=True).filter_by(mature=True)]

        if lock:
            if pids:
                current_app.logger.info("Locking {} payout ids at retriever request."
                                        .format(len(pids)))
                (Payout.query.filter(Payout.id.in_(p[2] for p in pids))
                 .update({Payout.locked: True}, synchronize_session=False))
            db.session.commit()
    return s.dumps([pids, lock])


@main.route("/update_payouts", methods=['POST'])
def update_transactions():
    """ Used as a response from an rpc payout system. This will either reset
    the locked status of a list of transactions upon failure on the remote
    side, or create a new CoinTransaction object and link it to the
    transactions to signify that the transaction has been processed. Both
    request and response are signed. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    data = s.loads(request.data)

    # basic checking of input
    try:
        if 'coin_txid' in data:
            assert len(data['coin_txid']) == 64
        else:
            assert 'reset' in data
            assert isinstance(data['reset'], bool)
        assert isinstance(data['pids'], list)
        assert isinstance(data['bids'], list)
        for id in data['pids']:
            assert isinstance(id, int)
        for id in data['bids']:
            assert isinstance(id, int)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to confirm", exc_info=True)
        abort(400)

    if data['reset']:
        with Benchmark("Resetting {:,} payouts and {:,} bonus payouts locked status"
                       .format(len(data['pids']), len(data['bids']))):
            if data['pids']:
                Payout.query.filter(Payout.id.in_(data['pids'])).update(
                    {Payout.locked: False}, synchronize_session=False)
            db.session.commit()
        return s.dumps(dict(success=True, result="Successfully reset"))

    return s.dumps(True)


@main.route("/confirm_transactions", methods=['POST'])
def confirm_transactions():
    """ Used to confirm that a transaction is now complete on the network. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    data = s.loads(request.data)

    # basic checking of input
    try:
        assert isinstance(data['tids'], list)
    except AssertionError:
        current_app.logger.warn("Invalid data passed to confirm_transactions",
                                exc_info=True)
        abort(400)

    Transaction.query.filter(Transaction.txid.in_(data['tids'])).update(
        {Transaction.confirmed: True}, synchronize_session=False)
    db.session.commit()

    return s.dumps(True)
