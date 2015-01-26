import argparse
import json
import os
import datetime

from simplecoin import create_manage_app, db, currencies, powerpools, redis_conn
from simplecoin.scheduler import SchedulerCommand
from simplecoin.models import (Transaction, UserSettings, Credit, ShareSlice,
                               DeviceSlice, Block, CreditExchange)

from urlparse import urlparse
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


@manager.option("--currency", type=str, dest="currency", default=None)
@manager.option('stop_id', type=int)
@manager.option('start_id', type=int)
def del_payouts(start_id, stop_id, currency=None):
    """
    Deletes payouts between start and stop id and removes their id from the
    associated Credits.

    Expects a start and stop payout id for payouts to be deleted

    If currency is passed, only payout matching that currency will be removed

    ::Warning:: This can really fuck things up!
    """
    from simplecoin.models import Payout
    payouts = Payout.query.filter(Payout.id >= start_id,
                                  Payout.id <= stop_id).all()

    if currency is not None:
        payouts = [payout for payout in payouts if payout.currency == currency]

    pids = [payout.id for payout in payouts]

    credits = Credit.query.filter(Credit.payout_id.in_(pids)).all()

    for credit in credits:
        credit.payout = None

        if credit.block and credit.block.orphan:
            credit.payable = False

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


@manager.option("currency", type=str)
def update_trade_requests(currency):
    """
    Looks at all uncompleted sell requests for a currency and
    re-looks at their credits.

    A Trade request's credits should always be payable - but this can
    get messed up if there is a long chain of orphans that is only later
    discovered (after the trade request is generated). This can happen from
    a daemon being on the wrong fork for a while, and then switching to the
    'official' fork.

    It is important that this function be run AFTER running update_block_state
    and del_payouts, which check the maturity of blocks & removes old incorrect
    payouts

    If any credits in the TR are discovered to now not be payable then subtract
    that credit amount from the TR.
    """
    from simplecoin.models import TradeRequest
    trs = TradeRequest.query.filter_by(_status=0, currency=currency,
                                       type="sell").all()

    adjustment = {}
    for tr in trs:
        for credit in tr.credits[:]:
            if credit.payable is False:
                print "Found unpayable credit for {} {} on TR #{}".format(credit.amount, credit.currency, tr.id)
                tr.quantity -= credit.amount
                tr.credits.remove(credit)
                adjustment.setdefault(tr.id, 0)
                adjustment[tr.id] -= credit.amount

    if adjustment:
        print "Preparing to update TRs: {}.".format(adjustment)
    else:
        print "Nothing to update...exiting."
        exit(0)

    res = raw_input("Are you really sure you want to perform this update? [y/n] ")
    if res != "y" and res != "yes":
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


@manager.option('host')
def forward_coinservs(host):
    """ Given a hostname, connects to a remote and tunnels all coinserver ports
    to local ports. Useful for development testing. """
    args = [host, "-N"]
    for currency in currencies.itervalues():
        if not currency.coinserv:
            continue
        args.append("-L {0}:127.0.0.1:{0}"
                    .format(currency.coinserv.config['port']))

    for pp in powerpools.itervalues():
        parts = urlparse(pp.monitor_address)
        if parts.hostname not in ['localhost', '127.0.0.1']:
            continue

        args.append("-L {0}:127.0.0.1:{0}".format(parts.port))

    current_app.logger.info(("/usr/bin/ssh", "/usr/bin/ssh", args))
    os.execl("/usr/bin/ssh", "/usr/bin/ssh", *args)


@manager.option('-ds', '--dont-simulate', default=False,
                action="store_true")
def convert_unexchangeable(dont_simulate):
    """ Converts Credit exchanges for unexchangeable currencies to payout the
    pool.

    XXX: Now broken due to config refactor """
    unexchangeable = []
    for currency in currencies.itervalues():
        # Skip unused currencies
        if not currency.coinserv:
            continue

        if not currency.exchangeable:
            unexchangeable.append((currency.key, currency.pool_payout))

    current_app.logger.info("Looking for CreditExchange's for currencies {}"
                            .format(unexchangeable))

    for key, pool_payout in unexchangeable:
        blocks = {}
        hashes = set()
        for ce in (CreditExchange.query.join(CreditExchange.block, aliased=True).
                   filter_by(currency=key)):
            blocks.setdefault(ce.block, [0, []])
            hashes.add(ce.block.hash)
            blocks[ce.block][0] += ce.amount
            blocks[ce.block][1].append(ce)
            db.session.delete(ce)

        # Sanity check, make sure block objs as keys is valid
        assert len(hashes) == len(blocks)

        for block, (amount, credits) in blocks.iteritems():
            # Create a new credit for the pool to displace the deleted
            # CreditExchanges. It will always be a credit since the currency is
            # unexchangeable
            pool_block = Credit(
                source=0,
                address=pool_payout['address'],
                user=pool_payout['user'],
                currency=pool_payout['currency'].key,
                amount=amount,
                block_id=block.id,
                payable=block.mature)
            db.session.add(pool_block)

            current_app.logger.info(
                "Block {} status {} value {} removed {} CreditExchanges of {} total amount"
                .format(block, block.status, block.total_value, len(credits), amount))

        current_app.logger.info("For currency {}, updated {} blocks"
                                .format(key, len(blocks)))

    if dont_simulate is True:
        current_app.logger.info("Committing transaction!")
        db.session.commit()
    else:
        current_app.logger.info("Rolling back!")
        db.session.rollback()


@manager.option('-t', '--txid', dest='transaction_id')
def confirm_trans(transaction_id):
    """ Manually confirms a transaction. Shouldn't be needed in normal use. """
    trans = Transaction.query.filter_by(txid=transaction_id).first()
    trans.confirmed = True
    db.session.commit()


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
