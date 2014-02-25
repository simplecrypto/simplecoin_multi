from flask import current_app
from . import coinserv


def payout_many(recip):
    fee = current_app.config['payout_fee']
    current_app.logger.debug("Setting tx fee: %s" % coinserv.settxfee(fee))
    wallet = coinserv.walletpassphrase(
        current_app.config['coinserv']['wallet_pass'], 10)
    current_app.logger.debug("Unlocking wallet: %s" % wallet)
    current_app.logger.info("Sending to recip: " + str(recip))
    return coinserv.sendmany(current_app.config['coinserv']['account'], recip)
