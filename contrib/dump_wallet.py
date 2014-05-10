""" This is a utility to empty an entire wallets balance and send it to
a specific address. It is useful for merged mining where getauxwork can't
specify a destination address and fills up local wallets. """
from bitcoinrpc import AuthServiceProxy, CoinRPCException
from decimal import Decimal
import logging
import argparse
import re


parser = argparse.ArgumentParser(prog='simplecoin wallet dump script')
parser.add_argument('-l', '--log-level',
                    choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                    default='INFO')
parser.add_argument('-s', '--simulate', action='store_true', default=False)
parser.add_argument('config_path', help='the path to your rpc server config file', type=file)
parser.add_argument('-p', '--passphrase-file', help='Path to a file containing your wallet passphrase', type=file)
parser.add_argument('-o', '--offset', help='The amount that will be shaved off '
                                            'the end to avoid stupid rounding error',
                    type=Decimal, default=Decimal("0.0001"))
parser.add_argument('recipient')
args = parser.parse_args()

root = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
root.addHandler(ch)
root.setLevel(getattr(logging, args.log_level))

config = {'pass': None}
for line in args.config_path:
    pts = line.strip().split("=")
    if len(pts) != 2:
        continue
    config[pts[0]] = pts[1]

if args.passphrase_file:
    config['pass'] = args.passphrase_file.read().strip()

rpc_connection = AuthServiceProxy(
    "http://{rpcuser}:{rpcpassword}@localhost:{rpcport}/"
    .format(**config))

try:
    balance = rpc_connection.getbalance()
except CoinRPCException as e:
    logging.error("Unable to retrieve balance, rpc returned {}".format(e))
    exit(1)

try:
    balance = Decimal(balance)
except ValueError:
    logging.error("Bogus data returned by balance call, exiting"
                  .format(str(balance)[:100]))
    exit(1)

if balance < Decimal("0.001"):
    logging.info("Balance to low to send!")


logging.info("Retrieved balance of {}.".format(balance))
balance -= args.offset
logging.info("Removing offset of {} for retrieved balance {} to avoid float "
             "rounding issues in JSON.".format(args.offset, balance))
if args.simulate:
    logging.info("Exiting for simulate")
    exit(0)

if config['pass']:
    try:
        wallet = rpc_connection.walletpassphrase(config['pass'], 10)
    except CoinRPCException as e:
        if e.error['code'] == -17:
            logging.info("Wallet already unlocked!")
        else:
            raise
    else:
        logging.info("Unlocking wallet: %s" % wallet)

fee = Decimal("0")
tries = 0
while tries < 3:
    try:
        send = rpc_connection.sendtoaddress(args.recipient, float(balance - fee))
    except CoinRPCException as e:
        if e.error['code'] == -4:
            try:
                matches = re.findall(r'[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?',
                                    e.error['message'])
                fee_match = Decimal(matches[0])
            except Exception:
                logging.error("Unable to parse fee requirement from!", exc_info=True)
                exit(1)
            else:
                fee = fee_match
                logging.info("Parsed a fee amount of {:7f}, retrying after applying that fee".format(fee))
                continue
        else:
            logging.error("CoinRPC raised unknown exception", exc_info=True)
            logging.error("Unable try and send funds {}".format(e))
            exit(1)
    else:
        logging.info("Successfully sent {:7f} with a fee of {:7f} and a trimmed offset of {:7f}"
                     .format(balance, fee, args.offset))
        break
    tries += 1
