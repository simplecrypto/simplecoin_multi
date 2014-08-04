import os
import logging
import sys
import argparse
import time
import json
import six
import requests

from urlparse import urljoin
from cryptokit.base58 import get_bcaddress_version
from flask import current_app
from itsdangerous import TimedSerializer, BadData

from bitcoinrpc.authproxy import JSONRPCException, CoinRPCException
from . import create_app


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
                 max_age=10):
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

    def remote(self, url, method, max_age=None, signed=True, **kwargs):
        url = urljoin("http://0.0.0.0:9400/", url)
        logger.debug("Making request to {}".format(url))
        ret = getattr(requests, method)(url, timeout=270, **kwargs)
        if ret.status_code != 200:
            raise RPCException("Non 200 from remote: {}".format(ret.text))

        try:
            logger.debug("Got {} from remote".format(ret.text.encode('utf8')))
            if signed:
                return self.serializer.loads(ret.text, max_age or self.max_age)
            else:
                return ret.json()
        except BadData:
            current_app.logger.error("Invalid data returned from remote!", exc_info=True)
            raise RPCException("Invalid signature")

    def poke_rpc(self, conn):
        try:
            conn.getinfo()
        except JSONRPCException:
            raise RPCException("Coinserver not awake")

    def confirm_trans(self, simulate=False):
        res = self.get('api/transaction?__filter_by={"confirmed":false}', signed=False)
        if not res['success']:
            logger.error("Failure from remote: {}".format(res))
            return

        tids = []
        for obj in res['objects']:
            if obj['merged_type']:
                conn = merge_coinserv[obj['merged_type']]
                confirms = current_app.config['merged_cfg'][obj['merged_type']]['trans_confirmations']
            else:
                conn = coinserv
                confirms = current_app.config['trans_confirmations']
            logger.debug("Connecting to {} coinserv to lookup confirms for {}"
                         .format(obj['merged_type'] or 'main', obj['txid']))
            try:
                res = conn.gettransaction(obj['txid'])
            except CoinRPCException:
                logger.error("Unable to fetch txid {} from {} rpc server!"
                             .format(obj['txid'], obj['merged_type']))
            except Exception:
                logger.error("Unable to fetch txid {} from {} rpc server!"
                             .format(obj['txid'], obj['merged_type']), exc_info=True)
            else:
                if res['confirmations'] > confirms:
                    tids.append(obj['txid'])
                    logger.info("Confirmed txid {} with {} confirms"
                                .format(obj['txid'], res['confirmations']))

        data = {'tids': tids}
        self.post('confirm_transactions', data=data)

    def reset_trans_file(self, fo, simulate=False):
        vals = json.load(fo)
        self.reset_trans(simulate=simulate, **vals)

    def associate_trans_file(self, fo, simulate=False):
        vals = json.load(fo)
        self.associate_trans(simulate=simulate, **vals)

    def string_to_list(self, string):
        return [int(i.strip()) for i in string.split(',')]

    def reset_trans(self, pids, bids, simulate=False):
        if isinstance(pids, basestring):
            pids = self.string_to_list(pids)
        if isinstance(bids, basestring):
            bids = self.string_to_list(bids)

        data = {'pids': pids, 'bids': bids, 'reset': True}
        logger.info("Resetting {:,} bonus ids and {:,} payout ids."
                    .format(len(bids), len(pids)))
        if simulate:
            logger.info("Just kidding, we're simulating... Exit.")
            exit(0)

        self.post('update_payouts', data=data)

    def associate_trans(self, pids, bids, transaction_id, merged, simulate=False):
        if isinstance(pids, basestring):
            pids = self.string_to_list(pids)
        if isinstance(bids, basestring):
            bids = self.string_to_list(bids)
        data = {'coin_txid': transaction_id, 'pids': pids, 'bids': bids, 'merged': merged}
        logger.info("Associating {:,} payout ids and {:,} bonus ids with txid {}"
                    .format(len(pids), len(bids), transaction_id))

        if simulate:
            logger.info("Just kidding, we're simulating... Exit.")
            exit(0)

        if self.post('update_payouts', data=data):
            logger.info("Sucessfully associated!")
            return True
        logger.info("Failed to associate!")
        return False

    def proc_trans(self, simulate=False, merged=None, datadir=None):
        logger.info("Running payouts for merged = {}".format(merged))
        if merged:
            conn = merge_coinserv[merged]
            valid_address_versions = current_app.config['merged_cfg'][merged]['address_version']
        else:
            conn = coinserv
            valid_address_versions = current_app.config['address_version']
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

        if not len(pids) and not len(bids):
            logger.info("No payouts to process..")
            return

        if not simulate:
            backup_fname = os.path.join(os.path.abspath(datadir),
                                        'locked_ids.{}'.format(int(time.time())))
            fo = open(backup_fname, 'w')
            json.dump(dict(pids=pids, bids=bids), fo)
            fo.close()
            logger.info("Locked pid information stored at {0}. Call sc_rpc "
                        "reset_trans_file {0} to reset these transactions"
                        .format(backup_fname))

        logger.info("Recieved {:,} payouts and {:,} bonus payouts from the "
                    "server".format(len(pids), len(bids)))

        # builds two dictionaries, one that tracks the total payouts to a user,
        # and another that tracks all the payout ids (pids) giving that amount
        # to the user
        totals = {}
        pids = {}
        bids = {}
        for user, amount, id in payouts:
            if get_bcaddress_version(user) in valid_address_versions:
                totals.setdefault(user, 0)
                totals[user] += amount
                pids.setdefault(user, [])
                pids[user].append(id)
            else:
                logger.warn("User {} has been excluded due to invalid address"
                            .format(user))

        for user, amount, id in bonus_payouts:
            if get_bcaddress_version(user) in valid_address_versions:
                totals.setdefault(user, 0)
                totals[user] += amount
                bids.setdefault(user, [])
                bids[user].append(id)
            else:
                logger.warn("User {} has been excluded due to invalid address"
                            .format(user))

        # identify the users who meet minimum payout and format for sending
        # to rpc
        users = {user: float(amount) for user, amount in totals.iteritems()
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
        logger.info(users)

        logger.debug("Total bonus IDs")
        logger.debug(bids)
        logger.debug("Total payout IDs")
        logger.debug(pids)

        logger.info("List of payout ids to be committed")
        logger.info(committed_pids)
        logger.info("List of bonus payout ids to be committed")
        logger.info(committed_bids)

        if simulate:
            logger.info("Just kidding, we're simulating... Exit.")
            exit(0)

        try:
            # now actually pay them
            coin_txid = payout_many(users, merged=merged)
            #coin_txid = "1111111111111111111111111111111111111111111111111111111111111111"
        except CoinRPCException as e:
            if isinstance(e.error, dict) and e.error.get('message') == 'Insufficient funds':
                logger.error("Insufficient funds, reseting...")
                self.reset_trans(pids, bids)
            else:
                logger.error("Unkown RPC error, you'll need to manually reset the payouts", exc_info=True)

        else:
            associated = False
            try:
                logger.info("Got {} as txid for payout, now pushing result to server!"
                            .format(coin_txid))

                retries = 0
                while retries < 5:
                    try:
                        if self.associate_trans(committed_pids, committed_bids, coin_txid, merged=merged):
                            logger.info("Recieved success response from the server.")
                            associated = True
                            break
                    except Exception:
                        logger.error("Server returned failure response, retrying "
                                     "{} more times.".format(4 - retries), exc_info=True)
                    retries += 1
                    time.sleep(15)
            finally:
                if not associated:
                    backup_fname = os.path.join(os.path.abspath(datadir),
                                                'associated_ids.{}'.format(int(time.time())))
                    fo = open(backup_fname, 'w')
                    json.dump(dict(pids=committed_pids,
                                   bids=committed_bids,
                                   transaction_id=coin_txid,
                                   merged=merged), fo)
                    fo.close()
                    logger.info("Failed transaction_id association data stored in {0}. Call sc_rpc "
                                "associate_trans_file {0} to retry manually".format(backup_fname))


def entry():
    parser = argparse.ArgumentParser(prog='simplecoin RPC')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    parser.add_argument('-s', '--simulate', action='store_true', default=False)
    subparsers = parser.add_subparsers(title='main subcommands', dest='action')

    subparsers.add_parser('confirm_trans',
                          help='fetches unconfirmed transactions and tries to confirm them')
    proc = subparsers.add_parser('proc_trans',
                                 help='processes transactions locally by '
                                      'fetching from a remote server')
    proc.add_argument('-m', '--merged', default=None)
    proc.add_argument('-d', '--datadir', required=True,
                      help='a folder that data will be stored in for resetting failed transactions')
    reset = subparsers.add_parser('reset_trans',
                                  help='resets the lock state of a set of pids'
                                       ' and bids')
    reset.add_argument('pids')
    reset.add_argument('bids')
    reset_file = subparsers.add_parser('reset_trans_file',
                                       help='resets the lock state of a set of pids'
                                       ' and bids by providing json file')
    reset_file.add_argument('fo', type=argparse.FileType('r'))

    confirm = subparsers.add_parser('associate_trans',
                                    help='associates pids/bids with transactions')
    confirm.add_argument('pids')
    confirm.add_argument('bids')
    confirm.add_argument('transaction_id')
    confirm.add_argument('merged')
    confirm_file = subparsers.add_parser('associate_trans_file',
                                         help='associates bids/pids with a txid by providing json file')
    confirm_file.add_argument('fo', type=argparse.FileType('r'))
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
