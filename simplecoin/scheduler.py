import logging
import datetime
import time
import urllib3
import requests
import sqlalchemy
import setproctitle
import decorator
import argparse
import json

from sqlalchemy.sql import select
from cryptokit import bits_to_shares, bits_to_difficulty
from bitcoinrpc import CoinRPCException
from flask import current_app
from time import sleep
from apscheduler.scheduler import Scheduler
from apscheduler.threadpool import ThreadPool
from math import ceil, floor

from simplecoin import create_app
from simplecoin import db, coinserv, cache, merge_coinserv
from simplecoin.models import (
    Share, Block, OneMinuteShare, Payout, Transaction, Blob, FiveMinuteShare,
    Status, OneMinuteReject, OneMinuteTemperature, FiveMinuteReject,
    OneMinuteHashrate, Threshold, Event, DonationPercent, BonusPayout,
    FiveMinuteTemperature, FiveMinuteHashrate, FiveMinuteType, OneMinuteType,
    MergeAddress)

logger = logging.getLogger('apscheduler.scheduler')


@decorator.decorator
def crontab(func, *args, **kwargs):
    """ Handles rolling back SQLAlchemy exceptions to prevent breaking the
    connection for the whole scheduler. Also records timing information into
    the cache """

    t = time.time()
    res = None
    try:
        res = func(*args, **kwargs)
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error("SQLAlchemyError occurred, rolling back: {}".format(e))
        db.session.rollback()
    except Exception:
        logger.error("Unhandled exception in {}".format(func.__name__),
                     exc_info=True)

    t = time.time() - t
    cache.cache._client.hmset('cron_last_run_{}'.format(func.__name__),
                              dict(runtime=t, time=int(time.time())))
    return res


def get_sharemap(start_id, shares_to_fetch, chunk_size=None, sleep_interval=None):
    """ Give a share id to start at and a number of shares to fetch (round size),
    returns a map of {user_address: share_count} format and how many shares
    were actually accrued """
    # allow overridable configuration defaults
    chunk_size = chunk_size or current_app.config.get('sharemap_chunk_size', 10000)
    sleep_interval = sleep_interval or current_app.config.get('sharemap_sleep_interval', 0.1)
    # hold all user shares here
    user_shares = {}
    stop_id = Share.query.order_by(Share.id).first().id  # last share to look for
    if start_id is None:
        # start at beginning
        start_id = Share.query.order_by(Share.id.desc()).first().id
    # iterate through shares in newest to oldest order to find the share
    # id that is oldest needed id
    remain = shares_to_fetch
    rows = 0
    sleep_total = 0
    start_time = time.time()
    while remain > 0 and start_id > stop_id:
        res = (db.engine.execution_options(stream_results=True).
               execute(select([Share.shares, Share.user]).
                       order_by(Share.id.desc()).
                       where(Share.id > start_id - chunk_size).
                       where(Share.id <= start_id)))
        chunk = res.fetchall()
        logger.debug("Fetching rows {:,} to {:,}".format(start_id - chunk_size, start_id))
        for shares, user in chunk:
            rows += 1
            user_shares.setdefault(user, 0)
            if remain > shares:
                user_shares[user] += shares
                remain -= shares
            else:
                user_shares[user] += remain
                remain = 0
                break
        else:
            # grab another batch, we've still got shares to find
            sleep(sleep_interval)
            sleep_total += sleep_interval
            start_id -= 10000
            continue

        # if we broke from the for loop we're done, exit
        break

    logger.info("Slept for a total of {}"
                .format(datetime.timedelta(seconds=sleep_total)))
    logger.info("Queried and summed for a total of {}"
                .format(datetime.timedelta(seconds=time.time() - start_time - sleep_total)))
    logger.info("Iterated {:,} rows to find {:,} shares"
                .format(rows, shares_to_fetch))
    logger.info("Found {:,} unique users in share log".format(len(user_shares)))

    return user_shares, shares_to_fetch - remain


@crontab
def update_online_workers():
    """
    Grabs a list of workers from the running powerpool instances and caches
    them
    """
    users = {}
    for i, pp_config in enumerate(current_app.config['monitor_addrs']):
        mon_addr = pp_config['mon_address'] + '/clients'
        try:
            req = requests.get(mon_addr)
            data = req.json()
        except Exception:
            logger.warn("Unable to connect to {} to gather worker summary."
                        .format(mon_addr))
        else:
            for address, workers in data['clients'].iteritems():
                users.setdefault('addr_online_' + address, [])
                for d in workers:
                    users['addr_online_' + address].append((d['worker'], i))

    cache.set_many(users, timeout=480)


@crontab
def update_pplns_est():
    """
    Generates redis cached value for share counts of all users based on PPLNS window
    """
    logger.info("Recomputing PPLNS for users")
    # grab configured N
    mult = int(current_app.config['last_n'])
    # grab configured hashes in a n1 share
    hashes_per_share = current_app.config.get('hashes_per_share', 65536)
    # generate average diff from last 500 blocks
    diff = cache.get('difficulty_avg')
    if diff is None:
        logger.warn("Difficulty average is blank, can't calculate pplns estimate")
        return

    # Calculate the total shares to that are 'counted'
    total_shares = ((float(diff) * (2 ** 32)) / hashes_per_share) * mult

    # Loop through all shares, descending order, until we'd distributed the
    # shares
    user_shares, total_grabbed = get_sharemap(None, total_shares)
    user_shares = {'pplns_' + k: v for k, v in user_shares.iteritems()}

    cache.set('pplns_total_shares', total_grabbed, timeout=40 * 60)
    cache.set('pplns_cache_time', datetime.datetime.utcnow(), timeout=40 * 60)
    cache.set_many(user_shares, timeout=40 * 60)
    cache.set('pplns_user_shares', user_shares, timeout=40 * 60)


@crontab
def cache_user_donation():
    """
    Grab all user donations and loop through them then cache donation %
    """
    user_donations = {}
    # Build a dict of donation % to cache
    custom_donations = DonationPercent.query.all()
    for donation in custom_donations:
        user_donations.setdefault(donation.user, current_app.config['default_perc'])
        user_donations[donation.user] = donation.perc

    cache.set('user_donations', user_donations, timeout=1440 * 60)


@crontab
def update_block_state():
    """
    Loops through all immature and non-orphaned blocks.
    First checks to see if blocks are orphaned,
    then it checks to see if they are now matured.
    """
    # Select all immature & non-orphaned blocks
    immature = Block.query.filter_by(mature=False, orphan=False)
    for block in immature:
        logger.info("Checking state of {} block height {}"
                    .format(block.merged_type or "main", block.height))
        if block.merged_type:
            merged_cfg = current_app.config['merged_cfg'][block.merged_type]
            mature_diff = merged_cfg['block_mature_confirms']
            rpc = merge_coinserv[block.merged_type]
            name = merged_cfg['name']
        else:
            mature_diff = current_app.config['block_mature_confirms']
            rpc = coinserv
            name = "main"

        try:
            blockheight = rpc.getblockcount()
        except (urllib3.exceptions.HTTPError, CoinRPCException) as e:
            logger.error("Unable to communicate with {} RPC server: {}"
                         .format(name, e))
            continue

        # ensure that our RPC server has more than caught up...
        if blockheight - 10 < block.height:
            logger.info("Skipping block {}:{} because blockchain isn't caught up."
                        "Block is height {} and blockchain is at {}"
                        .format(block.height, block.hash, block.height, blockheight))
            continue

        logger.info("Checking block height: {}".format(block.height))
        # Check to see if the block hash exists in the block chain
        try:
            output = rpc.getblock(block.hash)
            logger.debug("Confirms: {}; Height diff: {}"
                         .format(output['confirmations'],
                                 blockheight - block.height))
        except urllib3.exceptions.HTTPError as e:
            logger.error("Unable to communicate with {} RPC server: {}"
                         .format(name, e))
            continue
        except CoinRPCException:
            logger.info("Block {}:{} not in coin database, assume orphan!"
                        .format(block.height, block.hash))
            block.orphan = True
        else:
            if output['confirmations'] > mature_diff:
                logger.info("Block {}:{} meets {} confirms, mark mature"
                            .format(block.height, block.hash, mature_diff))
                block.mature = True
            elif (blockheight - block.height) > mature_diff and output['confirmations'] < mature_diff:
                logger.info("Block {}:{} {} height ago, but not enough confirms. Marking orphan."
                            .format(block.height, block.hash, mature_diff))
                block.orphan = True

        db.session.commit()


@crontab
def cleanup(simulate=False, chunk_size=None, sleep_interval=None):
    """
    Finds all the shares that will no longer be used and removes them from
    the database.
    """
    t = time.time()
    # allow overridable configuration defaults
    chunk_size = chunk_size or current_app.config.get('cleanup_chunk_size', 10000)
    sleep_interval = sleep_interval or current_app.config.get('cleanup_sleep_interval', 1.0)

    diff = cache.get('difficulty_avg')
    if diff is None:
        logger.warn("Difficulty average is blank, can't safely cleanup")
        return
    # count all unprocessed blocks
    unproc_blocks = len(Block.query.filter_by(processed=False, merged_type=None).all())
    # make sure we leave the right number of shares for unprocessed block
    # to be distributed
    unproc_n = unproc_blocks * current_app.config['last_n']
    # plus our requested cleanup n for a safe margin
    cleanup_n = current_app.config.get('cleanup_n', 4) + current_app.config['last_n']
    # calculate how many n1 shares that is
    total_shares = int(round(((float(diff) * (2 ** 16)) * (cleanup_n + unproc_n))))

    logger.info("Chunk size {:,}; Sleep time {}".format(chunk_size, sleep_interval))
    logger.info("Unprocessed blocks: {}; {} Extra N kept".format(unproc_blocks, unproc_n))
    logger.info("Safety margin N from config: {}".format(cleanup_n))
    logger.info("Total shares being saved: {:,}".format(total_shares))
    # upper and lower iteration bounds
    start_id = Share.query.order_by(Share.id.desc()).first().id + 1
    stop_id = Share.query.order_by(Share.id).first().id
    logger.info("Diff between first share {:,} and last {:,}: {:,}"
                .format(stop_id, start_id, start_id - stop_id))

    rows = 0
    counted_shares = 0
    stale_id = 0
    # iterate through shares in newest to oldest order to find the share
    # id that is oldest required to be kept
    while counted_shares < total_shares and start_id > stop_id:
        res = (db.engine.execute(select([Share.shares, Share.id]).
                                 order_by(Share.id.desc()).
                                 where(Share.id >= start_id - chunk_size).
                                 where(Share.id < start_id)))
        chunk = res.fetchall()
        for shares, id in chunk:
            rows += 1
            counted_shares += shares
            if counted_shares >= total_shares:
                stale_id = id
                break
        logger.info("Fetched rows {:,} to {:,}. Found {:,} shares so far. Avg share/row {:,.2f}"
                    .format(start_id - chunk_size, start_id, counted_shares, counted_shares / rows))
        start_id -= chunk_size

    if not stale_id:
        logger.info("Stale ID is 0, deleting nothing.")
        return

    logger.info("Time to identify proper id {}"
                .format(datetime.timedelta(seconds=time.time() - t)))
    logger.info("Rows iterated to find stale id: {:,}".format(rows))
    logger.info("Cleaning all shares older than id {:,}, up to {:,} rows. Saving {:,} rows."
                .format(stale_id, stale_id - stop_id, stale_id - start_id))
    if simulate:
        logger.info("Simulate mode, exiting")
        return

    # To prevent integrity errors, all blocks linking to a share that's
    # going to be deleted needs to be updated to remove reference
    Block.query.filter(Block.last_share_id <= stale_id).update({Block.last_share_id: None})
    db.session.commit()

    total_sleep = 0
    total = stale_id - stop_id
    remain = total
    # delete all shares that are sufficiently old
    while remain > 0:
        bottom = stale_id - chunk_size
        res = (Share.query.filter(Share.id < stale_id).
                filter(Share.id >= bottom).delete(synchronize_session=False))
        db.session.commit()
        remain -= chunk_size
        logger.info("Deleted {:,} rows from {:,} to {:,}\t{:,.4f}\t{:,}"
                    .format(res, stale_id, bottom, remain * 100.0 / total, remain))
        stale_id -= chunk_size
        if res:  # only sleep if we actually deleted something
            sleep(sleep_interval)
            total_sleep += sleep_interval

    logger.info("Time to completion {}".format(datetime.timedelta(time.time() - t)))
    logger.info("Time spent sleeping {}".format(datetime.timedelta(seconds=total_sleep)))


@crontab
def run_payouts(simulate=False):
    """ Loops through all the blocks that haven't been paid out and attempts
    to pay them out """
    blocks = Block.query.filter_by(processed=False).order_by(Block.found_at)
    for block in blocks:
        logger.info("Attempting to payout block height {} on chain {}; hash {}"
                    .format(block.height, block.merged_type or "main", block.hash))
        if block.last_share_id is None:
            logger.warn("Can't process this block, it's shares have been deleted!")
            continue
        payout(hash=block.hash, simulate=simulate)


def payout(hash=None, simulate=False):
    """
    Calculates payouts for users from share records for the latest found block.
    """
    if simulate:
        logger.debug("Running in simulate mode, no commit will be performed")
        logger.setLevel(logging.DEBUG)

    # find the oldest un-processed block
    if hash:
        block = Block.query.filter_by(processed=False, hash=hash).first()
    else:
        block = (Block.query.filter_by(processed=False).order_by(Block.found_at).first())

    if block is None:
        logger.debug("No block found, exiting...")
        return

    logger.debug("Processing block height {}".format(block.height))

    mult = int(current_app.config['last_n'])
    total_shares = int((bits_to_shares(block.bits) * mult) / current_app.config.get('share_multiplier', 1))
    logger.debug("Looking for up to {} total shares".format(total_shares))
    if block.last_share_id is None:
        logger.error("Can't process this block, it's shares have been deleted!")
        return
    logger.debug("Identified last matching share id as {}".format(block.last_share_id))

    # if we found less than n, use what we found as the total
    user_shares, total_shares = get_sharemap(block.last_share_id, total_shares)
    logger.debug("Found {} shares".format(total_shares))
    if simulate:
        out = "\n".join(["\t".join((user, str((amount * 100.0) / total_shares), str((amount * block.total_value) // total_shares), str(amount))) for user, amount in user_shares.iteritems()])
        logger.debug("Share distribution:\nUSR\t%\tBLK_PAY\tSHARE\n{}".format(out))

    logger.debug("Distribute_amnt: {}".format(block.total_value))
    if block.merged_type:
        merge_cfg = current_app.config['merged_cfg'][block.merged_type]
        new_user_shares = {merge_cfg['donate_address']: 0}
        # build a map of regular addresses to merged addresses
        query = (MergeAddress.query.filter_by(merged_type=block.merged_type).
                 filter(MergeAddress.user.in_(user_shares.keys())))
        merge_addr_map = {m.user: m.merge_address for m in query}
        logger.debug("Looking up merged mappings for merged_type {}, found {}"
                     .format(block.merged_type, len(merge_addr_map)))

        for user in user_shares:
            merge_addr = merge_addr_map.get(user)
            # if this user didn't set a merged mining address
            if not merge_addr:
                # give the excess to the donation address if set to not
                # distribute unassigned
                if not merge_cfg['distribute_unassigned']:
                    new_user_shares[merge_cfg['donate_address']] += user_shares[user]
                else:
                    total_shares -= user_shares[user]
                continue

            new_user_shares.setdefault(merge_addr, 0)
            new_user_shares[merge_addr] += user_shares[user]

        user_shares = new_user_shares

    assert total_shares == sum(user_shares.itervalues())

    # Below calculates the truncated portion going to each miner. because
    # of fractional pieces the total accrued wont equal the disitrubte_amnt
    # so we will distribute that round robin the miners in dictionary order
    accrued = 0
    user_payouts = {}
    for user, share_count in user_shares.iteritems():
        user_payouts[user] = float(share_count * block.total_value) // total_shares
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

    logger.debug("Ran round robin distro {} times to finish distrib".format(i))

    # now handle donation or bonus distribution for each user
    donation_total = 0
    bonus_total = 0
    # dictionary keyed by address to hold donate/bonus percs and amnts
    user_perc_applied = {}
    user_perc = {}
    if not block.merged_type:
        default_perc = current_app.config.get('default_perc', 0)
        # convert our custom percentages that apply to these users into an
        # easy to access dictionary
        custom_percs = DonationPercent.query.filter(DonationPercent.user.in_(user_shares.keys()))
        custom_percs = {d.user: d.perc for d in custom_percs}
    else:
        default_perc = merge_cfg.get('default_perc', 0)
        custom_percs = {}

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
        logger.debug("Final payout distribution:\nUSR\tAMNT\n{}".format(out))
        db.session.rollback()
    else:
        # record the payout for each user
        for user, amount in user_payouts.iteritems():
            if amount == 0.0:
                logger.info("Skip zero payout for USR: {}".format(user))
                continue
            Payout.create(user, amount, block, user_shares[user],
                          user_perc[user], user_perc_applied.get(user, 0),
                          merged_type=block.merged_type)
        # update the block status and collected amounts
        block.processed = True
        block.donated = donation_total
        block.bonus_payed = bonus_total
        # record the donations as a bonus payout to the donate address
        if donation_total > 0:
            if block.merged_type:
                donate_address = merge_cfg['donate_address']
            else:
                donate_address = current_app.config['donate_address']
            BonusPayout.create(donate_address, donation_total,
                                "Total donations from block {}"
                                .format(block.height), block,
                                merged_type=block.merged_type)
            logger.info("Added bonus payout to donation address {} for {}"
                        .format(donate_address, donation_total / 100000000.0))

        block_bonus = current_app.config.get('block_bonus', 0)
        if block_bonus > 0 and not block.merged_type:
            BonusPayout.create(block.user, block_bonus,
                               "Blockfinder bonus for block {}"
                               .format(block.height), block)
            logger.info("Added bonus payout for blockfinder {} for {}"
                        .format(block.user, block_bonus / 100000000.0))

        db.session.commit()


@crontab
def compress_minute():
    """ Compresses OneMinute records (for temp, hashrate, shares, rejects) to
    FiveMinute """
    OneMinuteShare.compress()
    OneMinuteReject.compress()
    OneMinuteTemperature.compress()
    OneMinuteHashrate.compress()
    OneMinuteType.compress()
    db.session.commit()


@crontab
def compress_five_minute():
    FiveMinuteShare.compress()
    FiveMinuteReject.compress()
    FiveMinuteTemperature.compress()
    FiveMinuteHashrate.compress()
    FiveMinuteType.compress()
    db.session.commit()


@crontab
def general_cleanup():
    """ Cleans up old database items.
    - Event for email rate limiting older than 1 hr.
    - Old status messages
    """
    now = datetime.datetime.utcnow()
    ten_hour_ago = now - datetime.timedelta(hours=12)
    one_hour_ago = now - datetime.timedelta(hours=1)
    Status.query.filter(Status.time < ten_hour_ago).delete()
    Event.query.filter(Event.time < one_hour_ago).delete()
    db.session.commit()

    sleep_interval = 0.0
    chunk_size = 100000
    one_week_ago = now - datetime.timedelta(days=7)
    logger.info("Removing all payouts older than {}".format(one_week_ago))
    start = Payout.query.filter(Payout.created_at < one_week_ago).order_by(Payout.id.desc()).first()
    if not start:
        current_app.logger.info("Payouts already cleaned up, exiting")
        return
    start_id = start.id + 1
    stop_id = Payout.query.filter(Payout.created_at < one_week_ago).order_by(Payout.id).first().id
    logger.info("Diff between first share {:,} and last {:,}: {:,}"
                .format(stop_id, start_id, start_id - stop_id))

    t = time.time()
    total_sleep = 0
    total = start_id - stop_id
    remain = total
    # delete all shares that are sufficiently old
    while remain > 0:
        bottom = start_id - chunk_size
        res = (Payout.query.filter(Payout.id < start_id).
               filter(Payout.created_at < one_week_ago).
               filter(Payout.id >= bottom).delete(synchronize_session=False))
        db.session.commit()
        remain -= chunk_size
        logger.info("Deleted {:,} rows from {:,} to {:,}\t{:,.4f}\t{:,}"
                    .format(res, start_id, bottom, remain * 100.0 / total, remain))
        start_id -= chunk_size
        if res:  # only sleep if we actually deleted something
            sleep(sleep_interval)
            total_sleep += sleep_interval

    logger.info("Time to completion {}".format(datetime.timedelta(seconds=time.time() - t)))
    logger.info("Time spent sleeping {}".format(datetime.timedelta(seconds=total_sleep)))
    db.session.commit()


@crontab
def update_network():
    """
    Queries the RPC servers confirmed to update network stats information.
    """
    def set_data(gbt, curr=None):
        prefix = ""
        if curr:
            prefix = curr + "_"
        prev_height = cache.get(prefix + 'blockheight') or 0

        if gbt['height'] == prev_height:
            logger.debug("Not updating {} net info, height {} already recorded."
                         .format(curr or 'main', prev_height))
            return
        logger.info("Updating {} net info for height {}.".format(curr or 'main', gbt['height']))

        # set general information for this network
        difficulty = bits_to_difficulty(gbt['bits'])
        cache.set(prefix + 'blockheight', gbt['height'], timeout=1200)
        cache.set(prefix + 'difficulty', difficulty, timeout=1200)
        cache.set(prefix + 'reward', gbt['coinbasevalue'], timeout=1200)

        # keep a configured number of blocks in the cache for getting average difficulty
        cache.cache._client.lpush(prefix + 'block_cache', gbt['bits'])
        cache.cache._client.ltrim(prefix + 'block_cache', 0, current_app.config['difficulty_avg_period'])
        diff_list = cache.cache._client.lrange(prefix + 'block_cache', 0, current_app.config['difficulty_avg_period'])
        total_diffs = sum([bits_to_difficulty(diff) for diff in diff_list])
        cache.set(prefix + 'difficulty_avg', total_diffs / len(diff_list), timeout=120 * 60)

        # add the difficulty as a one minute share, unless we're staging
        if not current_app.config.get('stage', False):
            now = datetime.datetime.utcnow()
            try:
                m = OneMinuteType(typ=prefix + 'netdiff', value=difficulty * 1000, time=now)
                db.session.add(m)
                db.session.commit()
            except sqlalchemy.exc.IntegrityError:
                db.session.rollback()
                slc = OneMinuteType.query.with_lockmode('update').filter_by(
                    time=now, typ=prefix + 'netdiff').one()
                # just average the diff of two blocks that occured in the same second..
                slc.value = ((difficulty * 1000) + slc.value) / 2
                db.session.commit()

    for merged_type, conf in current_app.config['merged_cfg'].iteritems():
        try:
            gbt = merge_coinserv[merged_type].getblocktemplate()
        except (urllib3.exceptions.HTTPError, CoinRPCException) as e:
            logger.error("Unable to communicate with {} RPC server: {}"
                            .format(conf['name'], e))
        else:
            set_data(gbt, curr=merged_type)

    try:
        gbt = coinserv.getblocktemplate()
    except (urllib3.exceptions.HTTPError, CoinRPCException) as e:
        logger.error("Unable to communicate with main RPC server: {}"
                        .format(e))
    else:
        set_data(gbt)


@crontab
def check_down():
    """
    Checks for latest OneMinuteShare from users that have a Threshold defined
    for their downtime.
    """
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


@crontab
def server_status():
    """
    Periodicly poll the backend to get number of workers and throw it in the
    cache
    """
    total_workers = 0
    servers = []
    raw_servers = {}
    for i, pp_config in enumerate(current_app.config['monitor_addrs']):
        mon_addr = pp_config['mon_address']
        try:
            req = requests.get(mon_addr)
            data = req.json()
        except Exception:
            logger.warn("Couldn't connect to internal monitor at {}"
                        .format(mon_addr))
            continue
        else:
            if 'server' in data:
                workers = data['stratum_manager']['client_count_authed']
                hashrate = data['stratum_manager']['mhps'] * 1000000
                raw_servers[pp_config['stratum']] = data
            else:
                workers = data['stratum_clients']
                hashrate = data['shares']['hour_total'] / 3600.0 * current_app.config['hashes_per_share']
            servers.append(dict(workers=workers,
                                hashrate=hashrate,
                                name=pp_config['stratum']))
            total_workers += workers

    cache.set('raw_server_status', json.dumps(raw_servers), timeout=1200)
    cache.set('server_status', json.dumps(servers), timeout=1200)
    cache.set('total_workers', total_workers, timeout=1200)


app = create_app(celery=True)


# monkey patch the thread pool for flask contexts
ThreadPool._old_run_jobs = ThreadPool._run_jobs
def _run_jobs(self, core):
    logger.debug("Starting patched threadpool worker!")
    with app.app_context():
        ThreadPool._old_run_jobs(self, core)
ThreadPool._run_jobs = _run_jobs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='simplecoin task scheduler')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    args = parser.parse_args()

    with app.app_context():
        root = logging.getLogger()
        hdlr = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
        hdlr.setFormatter(formatter)
        root.addHandler(hdlr)
        root.setLevel(getattr(logging, args.log_level))

        sched = Scheduler(standalone=True)
        logger.info("=" * 80)
        logger.info("SimpleCoin cron scheduler starting up...")
        setproctitle.setproctitle("simplecoin_scheduler")

        # All these tasks actually change the database, and shouldn't
        # be run by the staging server
        if not current_app.config.get('stage', False):
            # every hour at 10 minutes after
            sched.add_cron_job(cleanup, minute=10, second=45)
            # every two hours at 5 minutes after
            sched.add_cron_job(general_cleanup, hour='0,2,4,6,8,10,12,14,16,18,20,22', minute=5, second=30)
            # every minute at 55 seconds after the minute
            sched.add_cron_job(run_payouts, second=55)
            # every five minutes 20 seconds after the minute
            sched.add_cron_job(compress_minute, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=20)
            # every hour 2.5 minutes after the hour
            sched.add_cron_job(compress_five_minute, minute=2, second=30)
            # every 15 minutes 2 seconds after the minute
            sched.add_cron_job(update_block_state, minute='0,15,30,45', second=2)
            # every minute on the minute
            sched.add_cron_job(check_down, second=0)
        else:
            logger.info("Stage mode has been set in the configuration, not "
                        "running scheduled database altering cron tasks")

        sched.add_cron_job(update_online_workers, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=30)
        sched.add_cron_job(update_pplns_est, minute='0,15,30,45', second=2)
        sched.add_cron_job(cache_user_donation, minute='0,15,30,45', second=15)
        sched.add_cron_job(server_status, second=15)
        # every minute 10 seconds after the minute
        sched.add_cron_job(update_network, second=10)

        sched.start()
