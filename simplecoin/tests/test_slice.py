import datetime

from simplecoin.tests import UnitTest
from simplecoin.models import ValidShareSlice, gen_recent


class SliceTests(UnitTest):
    def test_span(self):
        start = datetime.datetime.utcnow()
        for x in xrange(300):
            now = start - datetime.timedelta(minutes=x)
            v = ValidShareSlice(time=now, user="test", worker="", algo="", span=0, value=x)
            self.db.session.add(v)
        self.db.session.commit()
        lower, upper = gen_recent()

        res = ValidShareSlice.get_span(stamp=True, lower=lower, upper=upper)
        self.assertEqual(len(res[0]['values']), 10)
        self.assertEqual(sum(res[0]['values'].values()), 55)

        res = ValidShareSlice.get_span(stamp=True)
        self.assertEqual(sum(res[0]['values'].values()), 44850)

    def test_compress(self):
        start = datetime.datetime.utcnow()
        for x in xrange(1500):
            now = start - datetime.timedelta(minutes=x)
            v = ValidShareSlice(time=now, user="test", worker="", algo="", span=0, value=x)
            self.db.session.add(v)
        self.db.session.commit()
        ValidShareSlice.compress(0)
        self.db.session.commit()
        lower, upper = gen_recent()

        res = ValidShareSlice.get_span(stamp=True, lower=lower, upper=upper)
        self.assertEqual(len(res[0]['values']), 10)
        self.assertEqual(sum(res[0]['values'].values()), 55)

        res = ValidShareSlice.get_span(stamp=True)
        self.assertEqual(sum(res[0]['values'].values()), 1124250)

        spans = {0: 0, 1: 0, 2: 0}
        for slc in ValidShareSlice.get_span(ret_query=True):
            spans[slc.span] += 1
        print spans
        assert spans[0] <= 61
        assert spans[1] >= 48

        ValidShareSlice.compress(1)
        self.db.session.commit()

        spans = {0: 0, 1: 0, 2: 0}
        for slc in ValidShareSlice.get_span(ret_query=True):
            spans[slc.span] += 1
        print spans
        assert spans[0] <= 61
        assert spans[1] <= 288
        assert spans[2] >= 1

        res = ValidShareSlice.get_span(stamp=True)
        self.assertEqual(sum(res[0]['values'].values()), 1124250)
