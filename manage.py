import argparse
import json
import datetime

from simplecoin import create_manage_app, db, currencies, powerpools, redis_conn
from simplecoin.scheduler import SchedulerCommand
from simplecoin.models import (Transaction, UserSettings, Credit, ShareSlice,
                               DeviceSlice, Block)

from flask import current_app, _request_ctx_stack
from flask.ext.migrate import stamp
from flask.ext.script import Manager, Shell, Server
from flask.ext.migrate import MigrateCommand


manager = Manager(create_manage_app)


@manager.option('-e', '--emit', help='prints the SQL that is executed',
                action="store_true")
def init_db(emit=False):
    """ Resets entire database to empty state """
    if emit:
        import logging
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    res = raw_input("You shouldn't probably ever do this in production! Are you"
                    " really, really sure you want to reset the DB {}? [y/n] "
                    .format(db.engine))
    if res != "y":
        return
    else:
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
    print "\n".join(["{0:.2f}% donation from {1} users"
                     .format(k * 100, v) for k, v in sorted(summ.items())])


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


@manager.command
def dump_effective_config():
    import pprint
    pprint.pprint(dict(current_app.config))


@manager.option('-t', '--txid', dest='transaction_id')
def confirm_trans(transaction_id):
    """ Manually confirms a transaction. Shouldn't be needed in normal use. """
    trans = Transaction.query.filter_by(txid=transaction_id).first()
    trans.confirmed = True
    db.session.commit()


@manager.option('simulate', help="When set to one, just print what would be deleted.")
@manager.option('oldest_kept', help="The oldest block hash that you want to save shares for")
@manager.option('chain', type=int, help="The chain on which to cleanup old shares")
@manager.option('-e', '--empty', type=int, default=20,
                help="Number of empty rows enountered before exiting")
def cleanup(chain, oldest_kept, simulate, empty):
    """ Given the oldest block hash that you desire to hold shares for, delete
    everything older than it. """
    for cp in Block.query.filter_by(hash=oldest_kept).one().chain_payouts:
        if cp.chainid == chain:
            oldest_kept = cp.solve_slice
            break

    current_app.logger.info(
        "Current slice index {}".format(redis_conn.get("chain_{}_slice_index"
                                                       .format(chain))))
    current_app.logger.info(
        "Looking at all slices older than {}".format(oldest_kept))

    simulate = bool(int(simulate))
    if not simulate:
        if raw_input("Are you sure you want to continue? [y/n]") != "y":
            return

    empty_found = 0
    for i in xrange(oldest_kept, 0, -1):
        if empty_found >= empty:
            current_app.logger.info("20 empty in a row, exiting")
            break
        key = "chain_{}_slice_{}".format(chain, i)
        if redis_conn.type(key) == 'none':
            empty_found += 1
        else:
            empty_found = 0

        if not simulate:
            current_app.logger.info("deleting {}!".format(key))
            current_app.logger.info(redis_conn.delete(key))
        else:
            current_app.logger.info("would delete {}".format(key))


def make_context():
    """ Setup a coinserver connection fot the shell context """
    app = _request_ctx_stack.top.app
    import simplecoin.models as m
    return dict(app=app, currencies=currencies, powerpools=powerpools, m=m, db=db)
manager.add_command("shell", Shell(make_context=make_context))


manager.add_command("runserver", Server())
manager.add_command('db', MigrateCommand)
manager.add_command('scheduler', SchedulerCommand)
manager.add_option('-c', '--config', dest='configs', action='append',
                   type=argparse.FileType('r'))
manager.add_option('-l', '--log-level',
                   choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default='INFO')


if __name__ == "__main__":
    manager.run()
