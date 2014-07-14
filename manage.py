import os
import logging
import datetime
import time
import sqlalchemy

from flask.ext.script import Manager, Shell
from flask.ext.migrate import Migrate, MigrateCommand
from simplecoin import create_app, db, coinservs, cache

app = create_app()
manager = Manager(app)
migrate = Migrate(app, db)

root = os.path.abspath(os.path.dirname(__file__) + '/../')

from bitcoinrpc.authproxy import AuthServiceProxy
from simplecoin.scheduler import (cleanup, run_payouts, server_status,
                                  update_online_workers, collect_minutes,
                                  cache_user_donation)
from simplecoin.models import (Transaction, DonationPercent, OneMinuteType,
                               FiveMinuteType, Block, Payout, TransactionSummary)
from simplecoin.utils import setfee_command
from flask import current_app, _request_ctx_stack

root = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
root.addHandler(ch)
root.setLevel(logging.DEBUG)

hdlr = logging.FileHandler(app.config.get('manage_log_file', 'manage.log'))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
root.addHandler(hdlr)
root.setLevel(logging.DEBUG)


@manager.command
def init_db():
    """ Resets entire database to empty state """
    with app.app_context():
        db.session.commit()
        db.drop_all()
        db.create_all()


@manager.command
def update_minimum_fee():
    """ Sets all custom fees in the database to be at least the minimum. Should
    be run after changing the minimum. """
    min_fee = current_app.config['minimum_perc']
    DonationPercent.query.filter(DonationPercent.perc < min_fee).update(
        {DonationPercent.perc: min_fee}, synchronize_session=False)
    db.session.commit()


@manager.option('fee')
@manager.option('user')
def set_fee(user, fee):
    """ Manually sets a fee percentage. """
    setfee_command(user, fee)


@manager.command
def list_fee_perc():
    """ Gives a summary of number of users at each fee amount """
    summ = {}
    warn = False
    for entry in DonationPercent.query.all():
        summ.setdefault(entry.perc, 0)
        summ[entry.perc] += 1
        if entry.perc < current_app.config['minimum_perc']:
            warn = True

    if warn:
        print("WARNING: A user is below the minimum configured value! "
              "Run update_minimum_fee command to resolve.")
    print "User fee summary"
    print "\n".join(["{0:+3d}% Fee: {1}".format(k, v) for k, v in sorted(summ.items())])


@manager.option('-s', '--simulate', dest='simulate', default=True)
def cleanup_cmd(simulate):
    """ Manually runs old share cleanup in simulate mode by default. """
    simulate = simulate != "0"
    cleanup(simulate=simulate)


@manager.command
def correlate_transactions():
    """ Derives transaction merged_type from attached payout's merged type.
    Intended as a migration assistant, not to be run for regular use. """
    for trans in Transaction.query:
        if trans.merged_type is None:
            payout = Payout.query.filter_by(transaction_id=trans.txid).first()
            if payout is not None:
                trans.merged_type = payout.merged_type
                current_app.logger.info("Updated txid {} to merged_type {}"
                                        .format(trans.txid, payout.merged_type))
            else:
                current_app.logger.info("Unable to get merged_type for txid "
                                        "{}, no payouts!".format(trans.txid))
    db.session.commit()


@manager.command
def generate_transaction_summaries():
    """ Looks through all the transactions that have no summaries and generates
    summaries for them. Used to upgrade to a summary based SQL. """
    total = len([t for t in Transaction.query if t.summaries == []])
    print "Looking to process {} total transactions that are missing summaries".format(total)
    for i, trans in enumerate(Transaction.query):
        if trans.summaries == []:
            user_amounts = {}
            user_counts = {}
            t = time.time()
            for payout in trans.payouts:
                user_counts.setdefault(payout.user, 0)
                user_amounts.setdefault(payout.user, 0)
                user_amounts[payout.user] += payout.amount
                user_counts[payout.user] += 1

            for user in user_counts:
                TransactionSummary.create(trans.txid, user, user_amounts[user],
                                          user_counts[user])

            print("{:.2f}% done. {:.2f} ms, total {} summaries, avg counts {}, txid {}, "
                  "merged_type {}".format(float(i) / total * 100,
                                          (time.time() - t) * 1000,
                                          len(user_counts),
                                          sum(user_counts.values()) / (len(user_counts) or 1),
                                          trans.txid,
                                          trans.merged_type))

        db.session.commit()


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


@manager.command
def wipe_fake_payout(merged_type="all"):
    """ If running the payout command in a way that pushes fake payout data
    for testing this will clear all the fake payout information """
    Payout.query.filter_by(transaction_id="1111111111111111111111111111111111111111111111111111111111111111").update({Payout.transaction_id: None})
    TransactionSummary.query.filter_by(transaction_id="1111111111111111111111111111111111111111111111111111111111111111").delete()
    Transaction.query.filter_by(txid="1111111111111111111111111111111111111111111111111111111111111111").delete()
    db.session.commit()


@manager.option('-t', '--txid', dest='transaction_id')
def confirm_trans(transaction_id):
    """ Manually confirms a transaction. Shouldn't be needed in normal use. """
    trans = Transaction.query.filter_by(txid=transaction_id).first()
    trans.confirmed = True
    db.session.commit()


@manager.command
def reload_cached():
    """ Recomputes all the cached values that normally get refreshed by tasks.
    Good to run if celery has been down, site just setup, etc. """
    update_online_workers()
    cache_user_donation()
    server_status()
    #from simplecoin.utils import get_block_stats
    #current_app.logger.info(
    #    "Refreshing the block stats (luck, effective return, orphan %)")
    #cache.delete_memoized(get_block_stats)
    #get_block_stats()


@manager.option('-s', '--simulate', dest='simulate', default=True)
def payout_cmd(simulate):
    """ Runs the payout task manually. Simulate mode is default. """
    simulate = simulate != "0"
    run_payouts(simulate=simulate)


@manager.command
def collect_minutes_cmd():
    """ Runs the collect minutes task manually. """
    collect_minutes()


def make_context():
    """ Setup a coinserver connection fot the shell context """
    app = _request_ctx_stack.top.app
    conn = AuthServiceProxy(
        "http://{0}:{1}@{2}:{3}/"
        .format(app.config['coinserv']['username'],
                app.config['coinserv']['password'],
                app.config['coinserv']['address'],
                app.config['coinserv']['port']))
    return dict(app=app, conn=conn)
manager.add_command("shell", Shell(make_context=make_context))
manager.add_command('db', MigrateCommand)


@manager.command
def runserver():
    current_app.run(debug=True, host='0.0.0.0')


if __name__ == "__main__":
    manager.run()
