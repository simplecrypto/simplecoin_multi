from flask import current_app

from . import coinserv, merge_coinserv


def payout_many(recip, merged=False):
    if merged:
        conn = merge_coinserv
    else:
        conn = coinserv
    if merged:
        fee = current_app.config['payout_fee']
        passphrase = current_app.config['coinserv']['wallet_pass']
    else:
        fee = current_app.config['merge']['payout_fee']
        passphrase = current_app.config['merge']['coinserv']['wallet_pass']
    if passphrase:
        wallet = conn.walletpassphrase(passphrase, 10)
        current_app.logger.info("Unlocking wallet: %s" % wallet)
    current_app.logger.info("Setting tx fee: %s" % conn.settxfee(fee))
    current_app.logger.info("Sending to recip: " + str(recip))
    return conn.sendmany(current_app.config['coinserv']['account'], recip)
