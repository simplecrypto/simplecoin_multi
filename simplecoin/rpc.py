import os
import logging
import sys
import argparse
import pprint
from urlparse import urljoin

import six
from flask import current_app
from itsdangerous import TimedSerializer, BadData

import requests
from bitcoinrpc.authproxy import JSONRPCException
from .coinserv_cmds import payout_many
from . import create_app, coinserv, merge_coinserv


logger = logging.getLogger("toroidal")
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

hdlr = logging.FileHandler('rpc.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


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
        url = urljoin(self.config['rpc_url'], url)
        ret = getattr(requests, method)(url, **kwargs)
        if ret.status_code != 200:
            raise RPCException("Non 200 from remote")

        try:
            logger.debug("Got {} from remote".format(ret.text))
            return self.serializer.loads(ret.text, max_age or self.max_age)
        except BadData:
            raise RPCException("Invalid signature: {}".format(ret.text))

    def poke_rpc(self, conn):
        try:
            conn.getinfo()
        except JSONRPCException:
            raise RPCException("Coinserver not awake")

    def reset_trans(self, pids, bids, simulate=False):
        proc_pids = []
        if pids:
            proc_pids = [int(i) for i in pids.split(',')]
        proc_bids = []
        if bids:
            proc_bids = [int(i) for i in bids.split(',')]
        data = {'pids': proc_pids, 'bids': proc_bids, 'reset': True}
        logger.info("Resetting requested bids and pids")
        self.post('update_payouts', data=data)

    def proc_trans(self, simulate=False, merged=False):
        logger.info("Running payouts for merged = {}".format(merged))
        if merged:
            conn = merge_coinserv
        else:
            conn = coinserv
        self.poke_rpc(conn)

        lock = True
        if simulate:
            lock = False

        payouts, bonus_payouts, lock_res = self.post(
            'get_payouts',
            data={'lock': lock, 'merged': merged}
        )
        if lock:
            assert lock_res

        pids = [t[2] for t in payouts]
        bids = [t[2] for t in bonus_payouts]
        if not simulate:
            logger.warn("Locked all recieved payout ids and bonus payout ids. In "
                        "the event of an error, run the following command to unlock"
                        "for a retried payout.\nsc_rpc reset_trans '{}' '{}'"
                        .format(",".join(str(p) for p in pids),
                                ",".join(str(b) for b in bids)))

        if not len(pids) and not len(bids):
            logger.info("No payouts to process..")
            return

        logger.info("Recieved {} payouts and {} bonus payouts from the server"
                    .format(len(pids), len(bids)))

        # builds two dictionaries, one that tracks the total payouts to a user,
        # and another that tracks all the payout ids (pids) giving that amount
        # to the user
        totals = {}
        pids = {}
        bids = {}
        if merged:
            valid_prefix = current_app.config['merge']['payout_prefix']
        else:
            valid_prefix = current_app.config['payout_prefix']
        for user, amount, id in payouts:
            if user.startswith(valid_prefix):
                totals.setdefault(user, 0)
                totals[user] += amount
                pids.setdefault(user, [])
                pids[user].append(id)
            else:
                logger.warn("User {} has been excluded due to invalid address"
                            .format(user))

        for user, amount, id in bonus_payouts:
            if user.startswith(valid_prefix):
                totals.setdefault(user, 0)
                totals[user] += amount
                bids.setdefault(user, [])
                bids[user].append(id)
            else:
                logger.warn("User {} has been excluded due to invalid address"
                            .format(user))

        # identify the users who meet minimum payout and format for sending
        # to rpc
        users = {user: amount / float(100000000) for user, amount in totals.iteritems()
                 if amount > current_app.config['minimum_payout']}
        logger.info("Trying to payout a total of {}".format(sum(users.values())))

        if len(users) == 0:
            logger.info("Nobody has a big enough balance to pay out...")
            return

        # now we have all the users who we're going to send money. build a list
        # of the pids that will be being paid in this transaction
        committed_pids = []
        for user in users:
            committed_pids.extend(pids.get(user, []))
        committed_bids = []
        for user in users:
            committed_bids.extend(bids.get(user, []))

        logger.info("Total user payouts")
        logger.info(pprint.pformat(users))
        logger.info("Total bonus IDs")
        logger.info(pprint.pformat(bids))
        logger.info("Total payout IDs")
        logger.info(pprint.pformat(pids))
        logger.info("List of payout ids to be committed")
        logger.info(committed_pids)
        logger.info("List of bonus payout ids to be committed")
        logger.info(committed_bids)

        if simulate:
            exit(0)

        # now actually pay them
        coin_txid = payout_many(users, merged=merged)
        #coin_txid = "1111111111111111111111111111111111111111111111111111111111111111"
        logger.info("Got {} as txid for payout!".format(coin_txid))

        data = {'coin_txid': coin_txid, 'pids': committed_pids, 'bids': committed_bids}
        logger.info("Sending data back to confirm_payouts: " + str(data))
        while True:
            try:
                if self.post('update_payouts', data=data):
                    logger.info("Recieved success response from the server.")
                    break
                else:
                    logger.error("Server returned failure response")
            except Exception:
                logger.error("Error recieved, press enter to retry",
                             exc_info=True)
                raw_input()


def entry():
    parser = argparse.ArgumentParser(prog='simplecoin RPC')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='WARN')
    parser.add_argument('-s', '--simulate', action='store_true', default=False)
    subparsers = parser.add_subparsers(title='main subcommands', dest='action')

    proc = subparsers.add_parser('proc_trans',
                                 help='processes transactions locally by '
                                      'fetching from a remote server')
    proc.add_argument('-m', '--merged', action='store_true', default=False)
    reset = subparsers.add_parser('reset_trans',
                                  help='resets the lock state of a set of pids'
                                       ' and bids')
    reset.add_argument('pids')
    reset.add_argument('bids')
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
