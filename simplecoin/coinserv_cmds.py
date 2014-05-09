from flask import current_app

from . import coinserv, merge_coinserv


def payout_many(recip, merged=None):
    if merged:
        merged_cfg = current_app.config['merged_cfg'][merged]
        conn = merge_coinserv[merged]

        fee = merged_cfg['payout_fee']
        passphrase = merged_cfg['coinserv']['wallet_pass']
        account = merged_cfg['coinserv']['account']
    else:
        conn = coinserv
        fee = current_app.config['payout_fee']
        passphrase = current_app.config['coinserv']['wallet_pass']
        account = current_app.config['coinserv']['account']

    if passphrase:
        wallet = conn.walletpassphrase(passphrase, 10)
        current_app.logger.info("Unlocking wallet: %s" % wallet)
    current_app.logger.info("Setting tx fee: %s" % conn.settxfee(fee))
    current_app.logger.info("Sending to recip: " + str(recip))
    current_app.logger.info("Sending from account: " + str(account))
    return conn.sendmany(account, recip)
