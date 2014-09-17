import argparse
import json
import datetime

from simplecoin import create_manage_app, db, currencies, powerpools, redis_conn
from simplecoin.scheduler import SchedulerCommand
from simplecoin.models import (Transaction, UserSettings, Credit, ShareSlice,
                               DeviceSlice, Block)

from flask import current_app, _request_ctx_stack
from flask.ext.migrate import stamp
from flask.ext.script import Manager, Shell
from flask.ext.migrate import MigrateCommand


manager = Manager(create_manage_app)


@manager.option('-e', '--emit', help='prints the SQL that is executed',
                action="store_true")
def init_db(emit=False):
    """ Resets entire database to empty state """
    if emit:
        import logging
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    db.session.commit()
    db.drop_all()
    db.create_all()
    stamp()


@manager.command
def list_donation_perc():
    """ Gives a summary of number of users at each donation amount """
    summ = {}
    warn = False
    for entry in UserSettings.query.all():
        summ.setdefault(entry.pdonation_perc, 0)
        summ[entry.pdonation_perc] += 1
        if entry.pdonation_perc < 0:
            warn = True

    if warn:
        print("WARNING: A user has set a donation percentage below 0!")
    print "User fee summary"
    print "\n".join(["{0:+3d}% Fee: {1}".format(k, v) for k, v in sorted(summ.items())])


@manager.option('-m', '--merged-type')
def reset_payouts(merged_type="all"):
    """ A utility that resets all payouts to an unlocked state. Use with care!
    Can in certain circumstances reset payouts that are already paid when
    pushes fail. """
    # regular
    base_q = Credit.query.filter_by(locked=True)
    if merged_type != "all":
        base_q = base_q.filter_by(merged_type=merged_type)
    base_q.update({Credit.locked: False})
    db.session.commit()


@manager.option('stop_id', type=int)
@manager.option('start_id', type=int)
def del_payouts(start_id, stop_id):
    """
    Deletes payouts between start and stop id and removes their id from the
    associated Credits.

    Expects a start and stop payout id for payouts to be deleted

    ::Warning:: This can really fuck things up!
    """
    from simplecoin.models import Payout
    payouts = Payout.query.filter(Payout.id >= start_id,
                                              Payout.id <= stop_id).all()

    pids = [payout.id for payout in payouts]

    credits = Credit.query.filter(Credit.payout_id.in_(pids)).all()

    for credit in credits:
        credit.payout = None

    db.session.flush()

    for payout in payouts:
        print "ID: {} ### USER: {} ### CREATED: {} ### AMOUNT: {} ### " \
              "CREDIT_COUNT: {}".format(payout.id, payout.user,
                                        payout.created_at, payout.amount,
                                        payout.count)
        db.session.delete(payout)

    print "Preparing to delete {} Payouts.".format(len(pids))

    res = raw_input("Are you really sure you want to delete these payouts? [y/n] ")
    if res != "y":
        db.session.rollback()
        return

    db.session.commit()


@manager.option('input', type=argparse.FileType('r'))
def import_shares(input):
    for i, line in enumerate(input):
        data = json.loads(line)
        data['time'] = datetime.datetime.utcfromtimestamp(data['time'])
        slc = ShareSlice(algo="scrypt", **data)
        floored = DeviceSlice.floor_time(data['time'], data['span'])
        if data['time'] != floored:
            current_app.logger.warn("{} != {}".format(data['time'], floored))
        data['time'] = floored
        db.session.add(slc)
        if i % 100 == 0:
            print "{} completed".format(i)
            db.session.commit()


@manager.option('input', type=argparse.FileType('r'))
def import_device_slices(input):
    for i, row in enumerate(input):
        data = json.loads(row)
        data['time'] = datetime.datetime.utcfromtimestamp(data['time'])
        data['stat'] = data.pop('_stat')
        # Do a basic integrity check
        floored = DeviceSlice.floor_time(data['time'], data['span'])
        if data['time'] != floored:
            current_app.logger.warn("{} != {}".format(data['time'], floored))
        data['time'] = floored
        db.session.add(DeviceSlice(**data))
        # Print periodic progress
        if i % 100 == 0:
            db.session.commit()
            print("{} inserted!".format(i))


@manager.option('-t', '--txid', dest='transaction_id')
def confirm_trans(transaction_id):
    """ Manually confirms a transaction. Shouldn't be needed in normal use. """
    trans = Transaction.query.filter_by(txid=transaction_id).first()
    trans.confirmed = True
    db.session.commit()


@manager.option('fees')
@manager.option('exchanged_quantity')
@manager.option('id')
def update_tr(id, exchanged_quantity, fees):
    """
    Updates a TR by posting to the RPC view
    """
    sc_rpc = RPCClient()
    completed_tr = {}

    completed_tr[id] = (exchanged_quantity, fees)

    sc_rpc.post(
        'update_trade_requests',
        data={'update': True, 'completed_trs': completed_tr}
    )


@manager.option('simulate')
@manager.option('oldest_kept')
@manager.option('chain', type=int)
def cleanup(chain, oldest_kept, simulate):
    for cp in Block.query.filter_by(hash=oldest_kept).one().chain_payouts:
        if cp.chainid == chain:
            oldest_kept = cp.solve_slice
            break

    print "Current slice index {}".format(redis_conn.get("chain_1_slice_index"))
    print "Looking at all slices older than {}".format(oldest_kept)

    simulate = bool(int(simulate))
    if not simulate:
        if raw_input("Are you sure you want to continue? [y/n]") != "y":
            return

    empty = 0
    for i in xrange(oldest_kept, 0, -1):
        if empty >= 20:
            print "20 empty in a row, exiting"
            break
        key = "chain_{}_slice_{}".format(chain, i)
        if not redis_conn.llen(key):
            empty += 1
        else:
            empty = 0

        if not simulate:
            print "deleting {}!".format(key)
            print redis_conn.delete(key)
        else:
            print "would delete {}".format(key)


def make_context():
    """ Setup a coinserver connection fot the shell context """
    app = _request_ctx_stack.top.app
    import simplecoin.models as m
    return dict(app=app, currencies=currencies, powerpools=powerpools, m=m)
manager.add_command("shell", Shell(make_context=make_context))
manager.add_command('db', MigrateCommand)
manager.add_command('scheduler', SchedulerCommand)
manager.add_option('-c', '--config', default='config.yml')
manager.add_option('-l', '--log-level',
                   choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default='INFO')


@manager.command
def runserver():
    current_app.run(debug=True, host='0.0.0.0')


if __name__ == "__main__":
    manager.run()
