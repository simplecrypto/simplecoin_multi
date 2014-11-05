from datetime import datetime, timedelta
import unittest

from simplecoin.models import make_upper_lower, ShareSlice

class TestUtil(unittest.TestCase):
    def test_make_upper_lower(self):
        print "now = %s" % datetime.utcnow()
        lower_10, upper_10 = make_upper_lower(offset=timedelta(minutes=2))
        print "lower_10 = %s, upper_10 = %s" % (lower_10, upper_10)

        lower_day, upper_day = make_upper_lower(span=timedelta(days=1),
                                            clip=timedelta(minutes=2))
        print "lower_day = %s, upper_day = %s" % (lower_day, upper_day)

    def test_floor_time(self):
        stamp = 1414135740
        minute = datetime.utcfromtimestamp(float(stamp))
        print "utcfromtimestamp = %s" % minute
        minute = ShareSlice.floor_time(minute, 0)
        print "floor_time = %s" % minute