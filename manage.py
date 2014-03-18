import os
import logging

from flask.ext.script import Manager, Shell
from flask.ext.migrate import Migrate, MigrateCommand
from simpledoge import create_app, db

app = create_app()
manager = Manager(app)
migrate = Migrate(app, db)

root = os.path.abspath(os.path.dirname(__file__) + '/../')

from bitcoinrpc.authproxy import AuthServiceProxy
from simpledoge.tasks import add_share, cleanup, payout
from simpledoge.models import Transaction, Threshold
from flask import current_app, _request_ctx_stack

root = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
root.addHandler(ch)
root.setLevel(logging.DEBUG)


@manager.command
def init_db():
    with app.app_context():
        db.session.commit()
        db.drop_all()
        db.create_all()


@manager.command
def power_shares():
    for i in xrange(2000000):
        add_share.delay('mrCuJ1WNXGpcBd8FA6H2cSeQLLXYuJ3qVt', 16)


@manager.option('-s', '--simulate', dest='simulate', default=True)
def cleanup_cmd(simulate):
    simulate = simulate != "0"
    cleanup(simulate=simulate)


@manager.option('-t', '--txid', dest='transaction_id')
def confirm_trans(transaction_id):
    trans = Transaction.query.filter_by(txid=transaction_id).first()
    trans.confirmed = True
    db.session.commit()


@manager.command
def test_email():
    thresh = Threshold(emails=['simpledogepool@gmail.com'])
    thresh.report_condition("Test condition")


@manager.option('-s', '--simulate', dest='simulate', default=True)
def payout_cmd(simulate):
    simulate = simulate != "0"
    payout(simulate=simulate)


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
