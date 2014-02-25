import requests
import os
import six
import logging
import sys
import argparse

from time import sleep
from flask import current_app
from urlparse import urljoin
from itsdangerous import TimedSerializer, BadData
from bitcoinrpc.authproxy import JSONRPCException

from .coinserv_cmds import payout_many
from . import create_app, coinserv


logger = logging.getLogger("toroidal")
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class RPCException(Exception):
    pass


class RPCClient(object):
    def __init__(self, config_path='/config.yml', root_suffix='/../',
                 max_age=5):
        self.root = os.path.abspath(os.path.dirname(__file__) + root_suffix)
        self.config = current_app.config
        del current_app.logger.handlers[0]
        current_app.logger.addHandler(ch)

        self.serializer = TimedSerializer(self.config['rpc_signature'])
        self.max_age = max_age

    def post(self, url, *args, **kwargs):
        if 'data' not in kwargs:
            kwargs['data'] = ''
        kwargs['data'] = self.serializer.dumps(kwargs['data'])
        return self.remote(url, 'post', *args, **kwargs)

    def get(self, url, *args, **kwargs):
        return self.remote(url, 'get', *args, **kwargs)

    def remote(self, url, method, max_age=None, **kwargs):
        url = urljoin(self.config['url'], url)
        ret = getattr(requests, method)(url, **kwargs)
        if ret.status_code != 200:
            raise RPCException("Non 200 from remote")

        try:
            return self.serializer.loads(ret.text, max_age or self.max_age)
        except BadData:
            raise RPCException("Invalid signature: {}".format(ret.text))

    def poke_rpc(self):
        try:
            coinserv.getinfo()
        except JSONRPCException:
            raise RPCException("Coinserver not awake")

    def proc_trans(self):
        self.poke_rpc()

        payouts = self.post('get_payouts')
        pids = [t[2] for t in payouts]
        logger.debug("Recieved {} transactions from the server"
                     .format(len(pids)))
        if not len(pids):
            logger.debug("No payouts to process..")
            return

        # builds two dictionaries, one that tracks the total payouts to a user,
        # and another that tracks all the payout ids (pids) giving that amount
        # to the user
        totals = {}
        pids = {}
        for user, amount, id in payouts:
            totals.setdefault(user, 0)
            totals[user] += amount
            pids.setdefault(user, [])
            pids[user].append(id)

        # identify the users who meet minimum payout and format for sending
        # to rpc
        users = {user: amount / float(100000000) for user, amount in totals.iteritems()
                 if amount > current_app.config['minimum_payout']}

        if len(users) == 0:
            logger.info("Nobody has a big enough balance to pay out...")
            return

        # now we have all the users who we're going to send money. build a list
        # of the pids that will be being paid in this transaction
        committed_pids = []
        for user in users:
            committed_pids.extend(pids[user])

        # now actually pay them
        coin_txid = payout_many(users)
        logger.debug("Got {} as txid for payout!".format(coin_txid))

        data = {'coin_txid': coin_txid, 'pids': committed_pids}
        logger.debug("Sending data back to confirm_payouts: " + str(data))
        while True:
            try:
                if self.post('confirm_payouts', data=data):
                    logger.info("Recieved success response from the server.")
                else:
                    logger.error("Server returned failure response")
            except Exception:
                logger.error("Error recieved, press enter to retry",
                             exc_info=True)
                raw_input()


def entry():
    parser = argparse.ArgumentParser(prog='toro')
    parser.add_argument('-l',
                        '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='WARN')
    subparsers = parser.add_subparsers(title='main subcommands', dest='action')

    subparsers.add_parser('proc_trans',
                          help='processes transactions locally by fetching '
                               'from a remote server')
    args = parser.parse_args()

    ch.setLevel(getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))

    global_args = ['log_level', 'action']
    # subcommand functions shouldn't recieve arguments directed at the
    # global object/ configs
    kwargs = {k: v for k, v in six.iteritems(vars(args)) if k not in global_args}

    app = create_app()
    with app.app_context():
        interface = RPCClient()
        try:
            getattr(interface, args.action)(**kwargs)
        except requests.exceptions.ConnectionError:
            logger.error("Couldn't connect to remote server", exc_info=True)
        except JSONRPCException as e:
            logger.error("Recieved exception from rpc server: {}"
                         .format(getattr(e, 'error')))
