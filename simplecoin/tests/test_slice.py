import datetime

from simplecoin.tests import UnitTest
from simplecoin.models import ShareSlice, make_upper_lower


class SliceTests(UnitTest):
    slice_test_data = dict(user="test", worker="", algo="", span=0,
                           share_type="acc")

    def test_span(self):
        start = datetime.datetime.utcnow()
        shares = list(xrange(300))
        for x in shares:
            now = start - datetime.timedelta(minutes=x)
            v = ShareSlice(time=now, value=x, **self.slice_test_data)
            self.db.session.add(v)
        self.db.session.commit()
        lower, upper = make_upper_lower()

        res = ShareSlice.get_span(stamp=True, lower=lower, upper=upper)
        self.assertEqual(len(res[0]['values']), 10)
        self.assertEqual(sum(res[0]['values'].values()), 45)

        res = ShareSlice.get_span(stamp=True)
        self.assertEqual(sum(res[0]['values'].values()), 44850)

    def test_compress(self):
        start = datetime.datetime.utcnow()
        for x in xrange(1500):
            now = start - datetime.timedelta(minutes=x)
            v = ShareSlice(time=now, value=x, **self.slice_test_data)
            self.db.session.add(v)
        self.db.session.commit()
        ShareSlice.compress(0)
        self.db.session.commit()
        lower, upper = make_upper_lower()

        res = ShareSlice.get_span(stamp=True, lower=lower, upper=upper)
        self.assertEqual(len(res[0]['values']), 10)
        self.assertEqual(sum(res[0]['values'].values()), 45)

        res = ShareSlice.get_span(stamp=True)
        self.assertEqual(sum(res[0]['values'].values()), 1124250)

        spans = {0: 0, 1: 0, 2: 0}
        for slc in ShareSlice.get_span(ret_query=True):
            spans[slc.span] += 1
        print spans
        assert spans[0] <= 60
        assert spans[1] >= 48

        ShareSlice.compress(1)
        self.db.session.commit()

        spans = {0: 0, 1: 0, 2: 0}
        for slc in ShareSlice.get_span(ret_query=True):
            spans[slc.span] += 1
        print spans
        assert spans[0] <= 61
        assert spans[1] <= 288
        assert spans[2] >= 1

        res = ShareSlice.get_span(stamp=True)
        self.assertEqual(sum(res[0]['values'].values()), 1124250)
