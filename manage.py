import os
import logging
import random

from flask.ext.script import Manager, Shell
from simpledoge import create_app

app = create_app()
manager = Manager(app)

root = os.path.abspath(os.path.dirname(__file__) + '/../')

from bitcoinrpc.authproxy import AuthServiceProxy
from simpledoge import db
from simpledoge.tasks import add_share, cleanup, payout
from simpledoge.models import Payout, Block
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


@manager.option('-s', '--simulate', dest='simulate', default=True)
def payout_cmd(simulate):
    simulate = simulate != "0"
    payout(simulate=simulate)


@manager.command
def fake_payout():
    addresses = ['mrCuJ1WNXGpcBd8FA6H2cSeQLLXYuJ3qVt',
                 'mn3gzxzs8WASz8WCr4ZTun2BmHbdEXG2tc',
                 'mg6pkcz4XdEHeD1ofpKahCAkXbpnKbL1jB',
                 'mhADrHGjRhp4U4Zjext6QwRJrVM73xvCDn']
    blockheight = 1000
    for i in xrange(blockheight):
        block = Block.create(random.choice(addresses), i, 50000, 5000, '')
        block.mature = True
        for address in addresses:
            if 0 == random.randint(0, 1):
                Payout.create(address, random.randint(1000000000, 10000000000), block)
    db.session.commit()


def make_context():
    app = _request_ctx_stack.top.app
    conn = AuthServiceProxy(
        "http://{0}:{1}@{2}:{3}/"
        .format(app.config['coinserv']['username'],
                app.config['coinserv']['password'],
                app.config['coinserv']['address'],
                app.config['coinserv']['port']))
    return dict(app=app,
                conn=conn)
manager.add_command("shell", Shell(make_context=make_context))


@manager.command
def runserver():
    current_app.run(debug=True, host='0.0.0.0')


if __name__ == "__main__":
    manager.run()
