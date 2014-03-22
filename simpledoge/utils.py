import calendar
import datetime
import time

from flask import current_app
from bitcoinrpc import CoinRPCException

from . import db, coinserv
from .models import DonationPercent


class CommandException(Exception):
    pass


def get_typ(typ, address, window=True):
    """ Gets the latest slices of a specific size. window open toggles
    whether we limit the query to the window size or not. We disable the
    window when compressing smaller time slices because if the crontab
    doesn't run we don't want a gap in the graph. This is caused by a
    portion of data that should already be compressed not yet being
    compressed. """
    # grab the correctly sized slices
    base = db.session.query(typ).filter_by(user=address)
    if window is False:
        return base
    grab = typ.floor_time(datetime.datetime.utcnow()) - typ.window
    return base.filter(typ.time >= grab)


def compress_typ(typ, address, workers):
    for slc in get_typ(typ, address, window=False):
        slice_dt = typ.upper.floor_time(slc.time)
        stamp = calendar.timegm(slice_dt.utctimetuple())
        workers.setdefault(slc.worker, {})
        workers[slc.worker].setdefault(stamp, 0)
        workers[slc.worker][stamp] += slc.value


def setfee_command(username, perc):
    perc = round(float(perc), 2)
    if perc > 100.0 or perc < current_app.config['minimum_perc']:
        raise CommandException("Invalid perc passed")
    obj = DonationPercent(user=username, perc=perc)
    db.session.merge(obj)
    db.session.commit()


def verify_message(address, message, signature):
    commands = {'SETFEE': setfee_command}
    lines = message.split("\t")
    parts = lines[0].split(" ")
    command = parts[0]
    args = parts[1:]
    try:
        stamp = int(lines[1])
    except ValueError:
        raise Exception("Second line must be integer timestamp!")
    now = time.time()
    if abs(now - stamp) > 820:
        raise Exception("Signature has expired!")

    if command not in commands:
        raise Exception("Invalid command given!")

    current_app.logger.error("Attemting to validate message '{}' with sig '{}' for address '{}'"
                             .format(message, signature, address))

    try:
        res = coinserv.verifymessage(address, signature, message)
    except CoinRPCException:
        raise Exception("Rejected by RPC server!")
    except Exception:
        current_app.logger.error("Coinserver verification error!", exc_info=True)
        raise Exception("Unable to communicate with coinserver!")
    if res:
        try:
            commands[command](address, *args)
        except CommandException:
            raise
        except Exception:
            raise Exception("Invalid arguments provided to command!")
    else:
        raise Exception("Invalid signature! Coinserver returned " + str(res))
