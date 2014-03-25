from flask import current_app
from . import coinserv


def payout_many(recip):
    fee = current_app.config['payout_fee']
    current_app.logger.info("Setting tx fee: %s" % coinserv.settxfee(fee))
    passphrase = current_app.config['coinserv']['wallet_pass']
    if passphrase:
        wallet = coinserv.walletpassphrase(passphrase, 10)
        current_app.logger.info("Unlocking wallet: %s" % wallet)
    current_app.logger.info("Sending to recip: " + str(recip))
    return coinserv.sendmany(current_app.config['coinserv']['account'], recip)
