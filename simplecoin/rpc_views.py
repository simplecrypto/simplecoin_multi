from itsdangerous import TimedSerializer
from flask import current_app, request, abort

from .models import Transaction, Payout, BonusPayout
from .views import main
from . import db


@main.route("/get_payouts", methods=['POST'])
def get_payouts():
    """ Used by remote procedure call to retrieve a list of transactions to
    be processed. Transaction information is signed for safety. """
    s = TimedSerializer(current_app.config['rpc_signature'])
    args = s.loads(request.data)
    current_app.logger.info("get_payouts being called!")
    lock = False
    if isinstance(args, dict) and args['lock']:
        lock = True

    payouts = (Payout.query.filter_by(transaction_id=None, locked=False).
               join(Payout.block, aliased=True).filter_by(mature=True)).all()
    bonus_payouts = (BonusPayout.query.filter_by(transaction_id=None, locked=False).
                     join(BonusPayout.block, aliased=True).filter_by(mature=True)).all()

    pids = [(p.user, p.amount, p.id) for p in payouts]
    bids = [(p.user, p.amount, p.id) for p in bonus_payouts]

    if lock:
        current_app.logger.info("Locking pids and bids at retriever request.")
        for payout in payouts:
            payout.locked = True
        for payout in bonus_payouts:
            payout.locked = True
        db.session.commit()
    return s.dumps([pids, bids, lock])


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

    if 'coin_txid' in data:
        coin_trans = Transaction.create(data['coin_txid'])
        db.session.flush()
        Payout.query.filter(Payout.id.in_(data['pids'])).update(
            {Payout.transaction_id: coin_trans.txid}, synchronize_session=False)
        BonusPayout.query.filter(BonusPayout.id.in_(data['bids'])).update(
            {BonusPayout.transaction_id: coin_trans.txid}, synchronize_session=False)
        db.session.commit()
    elif data['reset']:
        Payout.query.filter(Payout.id.in_(data['pids'])).update(
            {Payout.locked: False}, synchronize_session=False)
        BonusPayout.query.filter(BonusPayout.id.in_(data['bids'])).update(
            {BonusPayout.locked: False}, synchronize_session=False)
        db.session.commit()
        return s.dumps(dict(success=True, result="Successfully reset"))

    return s.dumps(True)
