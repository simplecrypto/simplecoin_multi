import calendar
import datetime
import time

from . import db, coinserv


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

def verify(address, message, ):
    commands = ['SETFEE']
    lines = message.split("\n")
    parts = lines[0].split(" ")
    command = parts[0]
    args = parts[1:]
    try:
        stamp = int(lines[1])
    except ValueError:
        raise Exception("Second line must be integer timestamp!")
    now = time.time()
    if abs(now - stamp) > 120:
        raise Exception("Signature has expired!")
    if command not in commands:
        raise Exception("Invalid command given!")

    coinserv.verifymessage(address, signature, message)
