from flask import current_app
from celery import Celery
from simpledoge import db, coinserv, cache
from simpledoge.models import (
    Share, Block, OneMinuteShare, Payout, Transaction, Blob, last_block_time,
    last_block_share_id, FiveMinuteShare, Status, OneMinuteReject,
    OneMinuteTemperature, FiveMinuteReject, OneMinuteHashrate, Threshold,
    Event, DonationPercent, BonusPayout)
from sqlalchemy.sql import func
from cryptokit import bits_to_shares, bits_to_difficulty
from pprint import pformat
from bitcoinrpc import CoinRPCException
from celery.utils.log import get_task_logger
from math import ceil, floor

import requests
import json
import sqlalchemy
import logging
import datetime

logger = get_task_logger(__name__)
celery = Celery('simpledoge')


@celery.task(bind=True)
def update_pplns_est(self):
    """
    Generates redis cached value for share counts of all users based on PPLNS window
    """
    try:
        # grab configured N
        mult = int(current_app.config['last_n'])
        # generate average diff from last 500 blocks
        blobs = Blob.query.filter(Blob.key.in_(("server", "diff"))).all()
        diff = [b for b in blobs if b.key == "diff"][0].data['diff']
        # Calculate the total shares to that are 'counted'
        total_shares = ((float(diff) * (2 ** 16)) * mult)

        # Loop through all shares, descending order, until we'd distributed the shares
        remain = total_shares
        user_shares = {}
        for share in Share.query.order_by(Share.id.desc()).yield_per(5000):
            user_shares.setdefault('pplns_' + share.user, 0)
            if remain > share.shares:
                user_shares['pplns_' + share.user] += share.shares
                remain -= share.shares
            else:
                user_shares['pplns_' + share.user] += remain
                remain = 0
                break

        cache.set('pplns_cache_time', datetime.datetime.utcnow(), timeout=40 * 60)
        cache.set_many(user_shares, timeout=40 * 60)
        cache.set('pplns_user_shares', user_shares, timeout=40 * 60)

    except Exception as exc:
        logger.error("Unhandled exception in estimating pplns", exc_info=True)
        raise self.retry(exc=exc)


@celery.task(bind=True)
def update_coin_transaction(self):
    """
    Loops through all immature transactions
    """
    try:
        # Select all unconfirmed transactions
        unconfirmed = Transaction.query.filter_by(confirmed=False)
        for tx in unconfirmed:
            # Check to see if the transaction hash exists in the block chain
            try:
                t = coinserv.gettransaction(tx.id)
                if t.get('confirmations', 0) >= 6:
                    tx.confirmed = True
            except CoinRPCException:
                tx.confirmed = False

        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in update block status", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def update_block_state(self):
    """
    Loops through all immature and non-orphaned blocks.
    First checks to see if blocks are orphaned,
    then it checks to see if they are now matured.
    """
    mature_diff = current_app.config['block_mature_confirms']
    try:
        # Select all immature & non-orphaned blocks
        immature = (Block.query.filter_by(mature=False, orphan=False))
        blockheight = coinserv.getblockcount()
        for block in immature:
            logger.info("Checking block height: {}".format(block.height))
            # Check to see if the block hash exists in the block chain
            try:
                output = coinserv.getblock(block.hash)
                logger.debug("Confirms: {}; Height diff: {}"
                             .format(output['confirmations'],
                                     blockheight - block.height))
            except CoinRPCException:
                logger.info("Block {}:{} not in coin database, assume orphan!"
                            .format(block.height, block.hash))
                block.orphan = True
            else:
                if output['confirmations'] > mature_diff:
                    logger.info("Block {}:{} meets 120 confirms, mark mature"
                                .format(block.height, block.hash))
                    block.mature = True
                elif (blockheight - block.height) > mature_diff and output['confirmations'] < mature_diff:
                    logger.info("Block {}:{} 120 height ago, but not enough confirms. Marking orphan."
                                .format(block.height, block.hash))
                    block.orphan = True

        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in update block status", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def add_share(self, user, shares):
    """
    Adds a round share to postgresql

    user: should be a username/wallet address
    shares: should be an integer representation of n1 shares
    """
    try:
        Share.create(user=user, shares=shares)
        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in add share", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def add_block(self, user, height, total_value, transaction_fees, bits,
              hash_hex):
    """
    Insert a block & blockchain data

    user: should be a username/wallet address of who found block
    height: should be the height of the given block in the blockchain
    total_value: should be an integer representation of the value of the
                  newly discovered block. E.G.
                  DOGE = 2364681.04976814
                  network_value = 236468104976814

    transaction_fees: should be an integer amount awarded due to transactions
                     handled by the block. E.G.
                     transaction fees on new block = 6.5
                     transaction_fees = 650000000
    """
    logger.warn(
        "Recieved an add block notification!\nUser: {}\nHeight: {}\n"
        "Total Height: {}\nTransaction Fees: {}\nBits: {}\nHash Hex: {}"
        .format(user, height, total_value, transaction_fees, bits, hash_hex))
    try:
        last = last_block_share_id()
        block = Block.create(user, height, total_value, transaction_fees, bits,
                             hash_hex, time_started=last_block_time())
        db.session.flush()
        count = (db.session.query(func.sum(Share.shares)).
                 filter(Share.id > last).
                 filter(Share.id <= block.last_share_id).scalar()) or 128
        block.shares_to_solve = count
        db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in add_block", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def add_one_minute(self, user, valid_shares, minute, worker='', dup_shares=0,
                   low_diff_shares=0, stale_shares=0):
    """
    Adds a new single minute entry for a user

    minute: timestamp (int)
    shares: number of shares recieved over the timespan
    user: string of the user
    """
    def count_share(typ, amount, user_=user):
        logger.debug("Adding {} for {} of amount {}"
                     .format(typ.__name__, user_, amount))
        try:
            typ.create(user_, amount, minute, worker)
            db.session.commit()
        except sqlalchemy.exc.IntegrityError:
            db.session.rollback()
            typ.add_value(user_, amount, minute, worker)
            db.session.commit()

    try:
        # log their valid shares
        if valid_shares:
            count_share(OneMinuteShare, valid_shares)

        # we want to log how much of each type of reject for the whole pool
        if user == "pool":
            if low_diff_shares:
                count_share(OneMinuteReject, low_diff_shares, user_="pool_low_diff")
            if dup_shares:
                count_share(OneMinuteReject, dup_shares, user_="pool_dup")
            if stale_shares:
                count_share(OneMinuteReject, stale_shares, user_="pool_stale")

        # only log a total reject on a per-user basis
        else:
            total_reject = dup_shares + low_diff_shares + stale_shares
            if total_reject:
                count_share(OneMinuteReject, total_reject)
    except Exception as exc:
        logger.error("Unhandled exception in add_one_minute", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def new_block(self, blockheight, bits=None, reward=None):
    """
    Notification that a new block height has been reached.
    """
    logger.info("Recieved notice of new block height {}".format(blockheight))
    if not isinstance(blockheight, int):
        logger.error("Invalid block height submitted, must be integer")

    blob = Blob(key='block', data={'height': str(blockheight),
                                   'difficulty': str(bits_to_difficulty(bits)),
                                   'reward': str(reward)})
    db.session.merge(blob)
    db.session.commit()


@celery.task(bind=True)
def cleanup(self, simulate=False):
    """
    Finds all the shares that will no longer be used and removes them from
    the database.
    """
    try:
        # find the oldest un-processed block
        block = (Block.query.
                 filter_by(processed=False).
                 order_by(Block.height).first())
        if block is None:
            # sloppy hack to get the newest block that is processed
            block = (Block.query.
                     filter_by(processed=True).
                     order_by(Block.height.desc()).first())
            if block is None:
                logger.debug("No block found, exiting...")
                return

        mult = int(current_app.config['last_n'])
        # take our standard share count times two for a safe margin. divide
        # by 16 to get the number of rows, since a rows minimum share count
        # is 16
        shares = (bits_to_shares(block.bits) * mult) * 2
        id_diff = shares // 16
        # compute the id which earlier ones are safe to delete
        stale_id = block.last_share.id - id_diff
        if simulate:
            logger.info("Share for block computed: {}".format(shares // 2))
            logger.info("Share total margin computed: {}".format(shares))
            logger.info("Id diff computed: {}".format(id_diff))
            logger.info("Stale ID computed: {}".format(stale_id))
            exit(0)
        elif stale_id > 0:
            logger.info("Cleaning all shares older than {}".format(stale_id))
            # delete all shares that are sufficiently old
            Share.query.filter(Share.id < stale_id).delete(
                synchronize_session=False)
            db.session.commit()
        else:
            logger.info("Not cleaning anything, stale id less than zero")
    except Exception as exc:
        logger.error("Unhandled exception in cleanup", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def payout(self, simulate=False):
    """
    Calculates payouts for users from share records for found blocks.
    """
    try:
        if simulate:
            logger.debug("Running in simulate mode, no commit will be performed")
            logger.setLevel(logging.DEBUG)

        # find the oldest un-processed block
        block = (Block.query.
                 filter_by(processed=False).
                 order_by(Block.height).first())
        if block is None:
            logger.debug("No block found, exiting...")
            return

        logger.debug("Processing block height {}".format(block.height))

        mult = int(current_app.config['last_n'])
        # take our standard share count times two for a safe margin. divide
        # by 16 to get the number of rows, since a rows minimum share count
        # is 16
        total_shares = (bits_to_shares(block.bits) * mult)
        logger.debug("Looking for up to {} total shares".format(total_shares))
        remain = total_shares
        start = block.last_share.id
        logger.debug("Identified last matching share id as {}".format(start))
        user_shares = {}
        for share in Share.query.order_by(Share.id.desc()).filter(Share.id <= start).yield_per(100):
            user_shares.setdefault(share.user, 0)
            if remain > share.shares:
                user_shares[share.user] += share.shares
                remain -= share.shares
            else:
                user_shares[share.user] += remain
                remain = 0
                break

        # if we found less than n, use what we found as the total
        total_shares -= remain
        logger.debug("Found {} shares".format(total_shares))
        if simulate:
            out = "\n".join(["\t".join((user, str(amount))) for user, amount in user_shares.iteritems()])
            logger.debug("Share distribution:\n{}".format(out))

        # Calculate the portion going to the miners by truncating the
        # fractional portions and giving the remainder to the pool owner
        logger.debug("Distribute_amnt: {}".format(block.total_value))
        # Below calculates the truncated portion going to each miner. because
        # of fractional pieces the total accrued wont equal the disitrubte_amnt
        # so we will distribute that round robin the miners in dictionary order
        accrued = 0
        user_payouts = {}
        for user, share_count in user_shares.iteritems():
            user_payouts.setdefault(share.user, 0)
            user_payouts[user] = (share_count * block.total_value) // total_shares
            accrued += user_payouts[user]

        logger.debug("Total accrued after trunated iteration {}; {}%"
                     .format(accrued, (accrued / float(block.total_value)) * 100))
        # loop over the dictionary indefinitely until we've distributed
        # all the remaining funds
        i = 0
        while accrued < block.total_value:
            for key in user_payouts:
                i += 1
                user_payouts[key] += 1
                accrued += 1
                # exit if we've exhausted
                if accrued >= block.total_value:
                    break

        logger.debug("Ran round robin distro {} times to finish distrib"
                     .format(i))

        # now handle donation or bonus distribution for each user
        donation_total = 0
        bonus_total = 0
        user_perc_applied = {}
        user_perc = {}
        default_perc = current_app.config.get('default_perc', 0)
        # convert our custom percentages that apply to these users into an
        # easy to access dictionary
        custom_percs = DonationPercent.query.filter(DonationPercent.user.in_(user_shares.keys()))
        custom_percs = {d.user: d.perc for d in custom_percs}
        for user, payout in user_payouts.iteritems():
            # use the custom perc, or fallback to the default
            perc = custom_percs.get(user, default_perc)
            user_perc[user] = perc

            # if the perc is greater than 0 it's calced as a donation
            if perc > 0:
                donation = int(ceil((perc / 100.0) * payout))
                logger.debug("Donation of\t{}\t({}%)\tcollected from\t{}"
                             .format(donation / 100000000.0, perc, user))
                donation_total += donation
                user_payouts[user] -= donation
                user_perc_applied[user] = donation

            # if less than zero it's a bonus payout
            elif perc < 0:
                perc *= -1
                bonus = int(floor((perc / 100.0) * payout))
                logger.debug("Bonus of\t{}\t({}%)\tpaid to\t{}"
                             .format(bonus / 100000000.0, perc, user))
                user_payouts[user] += bonus
                bonus_total += bonus
                user_perc_applied[user] = -1 * bonus

            # percentages of 0 are no-ops

        logger.info("Payed out {} in bonus payment"
                    .format(bonus_total / 100000000.0))
        logger.info("Received {} in donation payment"
                    .format(donation_total / 100000000.0))
        logger.info("Net income from block {}"
                    .format((donation_total - bonus_total) / 100000000.0))

        assert accrued == block.total_value
        logger.info("Successfully distributed all rewards among {} users."
                    .format(len(user_payouts)))

        # run another safety check
        user_sum = sum(user_payouts.values())
        assert user_sum == (block.total_value + bonus_total - donation_total)
        logger.info("Double check for payout distribution."
                    " Total user payouts {}, total block value {}."
                    .format(user_sum, block.total_value))

        if simulate:
            out = "\n".join(["\t".join((user, str(amount / 100000000.0))) for user, amount in user_payouts.iteritems()])
            logger.debug("Payout distribution:\n{}".format(out))
            db.session.rollback()
        else:
            # record the payout for each user
            for user, amount in user_payouts.iteritems():
                Payout.create(user, amount, block, user_shares[user],
                              user_perc[user], user_perc_applied.get(user, 0))
            # update the block status and collected amounts
            block.processed = True
            block.donated = donation_total
            block.bonus_payed = bonus_total
            # record the donations as a bonus payout to the donate address
            if donation_total > 0:
                donate_address = current_app.config['donate_address']
                BonusPayout.create(donate_address, donation_total,
                                   "Total donations from block {}"
                                   .format(block.height))
                logger.info("Added bonus payout to donation address {} for {}"
                            .format(donate_address, donation_total / 100000000.0))

            block_bonus = current_app.config.get('block_bonus', 0)
            if block_bonus > 0:
                BonusPayout.create(block.user, block_bonus,
                                   "Blockfinder bonus for block {}"
                                   .format(block.height))
                logger.info("Added bonus payout for blockfinder {} for {}"
                            .format(block.user, block_bonus / 100000000.0))

            db.session.commit()
    except Exception as exc:
        logger.error("Unhandled exception in payout", exc_info=True)
        db.session.rollback()
        raise self.retry(exc=exc)


@celery.task(bind=True)
def compress_minute(self):
    """ Compresses OneMinute records (for temp, hashrate, shares, rejects) to
    FiveMinute """
    try:
        OneMinuteShare.compress()
        OneMinuteReject.compress()
        OneMinuteTemperature.compress()
        db.session.commit()
    except Exception:
        logger.error("Unhandled exception in compress_minute", exc_info=True)
        db.session.rollback()


@celery.task(bind=True)
def compress_five_minute(self):
    try:
        FiveMinuteShare.compress()
        FiveMinuteReject.compress()
        db.session.commit()
    except Exception:
        logger.error("Unhandled exception in compress_five_minute", exc_info=True)
        db.session.rollback()


@celery.task(bind=True)
def general_cleanup(self):
    """ Cleans up old database items.
    - Event for email rate limiting older than 1 hr.
    - Old status messages
    """
    try:
        now = datetime.datetime.utcnow()
        ten_hour_ago = now - datetime.timedelta(hours=12)
        one_hour_ago = now - datetime.timedelta(hours=1)
        Status.query.filter(Status.time < ten_hour_ago).delete()
        Event.query.filter(Event.time < one_hour_ago).delete()
        db.session.commit()
    except Exception:
        logger.error("Unhandled exception in remove_old_statuses", exc_info=True)
        db.session.rollback()


@celery.task(bind=True)
def agent_receive(self, address, worker, typ, payload, timestamp):
    """ Accepts ppagent data that is forwarded from powerpool and manages
    adding it to the database and triggering alerts as needed. """
    # convert unix timestamp to datetime
    dt = datetime.datetime.utcfromtimestamp(timestamp)

    def inject_device_stat(cls, device, value):
        if value:
            stat = cls(user=address, worker=worker, device=device, value=value,
                       time=dt)
            db.session.merge(stat)

    try:
        # if they passed a threshold we should update the database object
        if typ == "thresholds":
            try:
                thresh = Threshold(
                    worker=worker,
                    user=address,
                    green_notif=not payload.get('no_green_notif', False),
                    temp_thresh=payload.get('overheat'),
                    hashrate_thresh=payload.get('lowhashrate'),
                    offline_thresh=payload.get('offline'),
                    emails=payload['emails'][:4])
                db.session.merge(thresh)
            except KeyError:
                # key error means they sent bad data to us. This might be
                # ppagent's fault, but likely someone doing something goofy
                logger.warn("Bad payload was sent as Threshold data: {}"
                            .format(payload))
            db.session.commit()
            return
        elif typ == 'status':
            ret = (db.session.query(Status).filter_by(user=address, worker=worker).
                   update({"status": json.dumps(payload), "time": dt}))
            # if the update affected nothing
            if ret == 0:
                new = Status(user=address, worker=worker,
                             status=json.dumps(payload), time=dt)
                db.session.add(new)
            db.session.commit()
            return

        # the two status messages can trigger a threshold condition, so we need
        # to load the threshold to check
        thresh = Threshold.query.filter_by(worker=worker, user=address).first()
        if typ == 'temp':
            # track the overheated cards
            overheat_cards = []
            temps = []
            for i, value in enumerate(payload):
                inject_device_stat(OneMinuteTemperature, i, value)
                # report over temperature
                if thresh and value >= thresh.temp_thresh:
                    overheat_cards.append(str(i))
                    temps.append(str(value))

            if overheat_cards and not thresh.temp_err:
                s = "s" if len(overheat_cards) else ""
                thresh.report_condition(
                    "Worker {}, overheat on card{s} {}, temp{s} {}"
                    .format(worker, ', '.join(overheat_cards), ', '.join(temps),
                            s=s),
                    'temp_err', True)
            elif not overheat_cards and thresh and thresh.temp_err:
                thresh.report_condition(
                    "Worker {} overheat condition relieved".format(worker),
                    'temp_err', False)

        elif typ == 'hashrate':
            for i, value in enumerate(payload):
                # multiply by a million to turn megahashes to hashes
                inject_device_stat(OneMinuteHashrate, i, value * 1000000)

            # do threshold checking if they have one set
            if thresh:
                hr = sum(payload) * 1000
                if int(hr) == 0:
                    current_app.logger.warn("Entry with 0 hashrate. Worker {}; User {}".format(worker, address))
                else:
                    low_hash = thresh and hr <= thresh.hashrate_thresh
                    if low_hash and not thresh.hashrate_err:
                        thresh.report_condition(
                            "Worker {} low hashrate condition, hashrate {} KH/s"
                            .format(worker, hr), 'hashrate_err', True)
                    elif not low_hash and thresh.hashrate_err:
                        thresh.report_condition(
                            "Worker {} low hashrate condition resolved, hashrate {} KH/s"
                            .format(worker, hr), 'hashrate_err', False)
        else:
            logger.warning("Powerpool sent an unkown agent message of type {}"
                           .format(typ))
        db.session.commit()
    except Exception:
        logger.error("Unhandled exception in update_status", exc_info=True)
        db.session.rollback()


@celery.task(bind=True)
def check_down(self):
    """
    Checks for latest OneMinuteShare from users that have a Threshold defined
    for their downtime.
    """
    try:
        for thresh in Threshold.query.filter(Threshold.offline_thresh != None):
            last = Status.query.filter_by(worker=thresh.worker, user=thresh.user).first()
            if not last:
                continue
            diff = int((datetime.datetime.utcnow() - last.time).total_seconds() / 60)
            if not thresh.offline_err and diff > thresh.offline_thresh:
                thresh.report_condition("Worker {} offline for {} minutes"
                                        .format(thresh.worker, diff),
                                        'offline_err',
                                        True)

            # if there's an error registered and it's not showing offline
            elif thresh.offline_err and diff <= thresh.offline_thresh:
                thresh.report_condition("Worker {} now back online"
                                        .format(thresh.worker),
                                        'offline_err',
                                        False)

        db.session.commit()
    except Exception:
        logger.error("Unhandled exception in check_down", exc_info=True)
        db.session.rollback()


@celery.task(bind=True)
def server_status(self):
    """
    Periodic pull update of server stats
    """
    try:
        mon_addr = current_app.config['monitor_addr']
        try:
            req = requests.get(mon_addr)
            data = req.json()
        except Exception:
            logger.warn("Couldn't connect to internal monitor at {}".format(mon_addr),
                        exc_info=True)
            output = {'stratum_clients': 0, 'agent_clients': 0}
        else:
            output = {'stratum_clients': data['stratum_clients'],
                      'agent_clients': data['agent_clients']}
        blob = Blob(key='server', data={k: str(v) for k, v in output.iteritems()})
        db.session.merge(blob)
        db.session.commit()
    except Exception:
        logger.error("Unhandled exception in server_status", exc_info=True)
        db.session.rollback()


@celery.task(bind=True)
def difficulty_avg(self):
    """
    Setup a blob with the average network difficulty for the last 500 blocks
    """
    try:
        req = requests.get("http://dogechain.info/chain/Dogecoin/q/nethash/1/-500?format=json")
        data = req.json()
    except Exception as exc:
        logger.warn("Couldn't connect to dogechain.info", exc_info=True)
        raise self.retry(exc=exc)

    try:
        diffs = [col[4] for col in data]
        avg = sum(diffs) / len(diffs)
        blob = Blob(key='diff', data={'diff': str(avg)})
        db.session.merge(blob)
        db.session.commit()
    except Exception as exc:
        logger.warn("Unknown failure in difficulty_avg", exc_info=True)
        raise self.retry(exc=exc)
