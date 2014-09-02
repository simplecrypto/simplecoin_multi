import argparse
import json
import datetime

from flask import current_app, _request_ctx_stack
from flask.ext.migrate import stamp
from flask.ext.script import Manager, Shell
from flask.ext.migrate import MigrateCommand

from simplecoin import create_manage_app, db, currencies, powerpools
from simplecoin.scheduler import SchedulerCommand
from simplecoin.models import Transaction, UserSettings, Payout, ShareSlice, DeviceSlice


manager = Manager(create_manage_app)


@manager.command
def init_db():
    """ Resets entire database to empty state """
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
    base_q = Payout.query.filter_by(locked=True)
    if merged_type != "all":
        base_q = base_q.filter_by(merged_type=merged_type)
    base_q.update({Payout.locked: False})
    db.session.commit()


@manager.option('input', type=argparse.FileType('r'))
def import_shares(input):
    for i, line in enumerate(input):
        data = json.loads(line)
        data['time'] = datetime.datetime.utcfromtimestamp(data['time'])
        slc = ShareSlice(algo="scrypt", **data)
        db.session.add(slc)
        if i % 100 == 0:
            print "{} completed".format(i)
            db.session.commit()


@manager.option('input', type=argparse.FileType('r'))
def import_device_slices(input):
    for i, row in enumerate(input):
        data = json.loads(row)
        data['time'] = datetime.datetime.utcfromtimestamp(data['time'])
        # Do a basic integrity check
        assert data['time'] == DeviceSlice.floor_time(data['time'], data['span'])
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
