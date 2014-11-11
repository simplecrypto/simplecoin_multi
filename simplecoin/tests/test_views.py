import datetime

from simplecoin import db, cache
from simplecoin.scheduler import leaderboard
from simplecoin.utils import anon_users
from simplecoin.tests import RedisUnitTest, UnitTest

import simplecoin.models as m


class TestViewsRedis(RedisUnitTest):
    def test_basic_not_500(self):
        for view in ['/',
                     '/pool_stats',
                     '/configuration_guide',
                     '/stats',
                     '/leaderboard',
                     '/news',
                     '/blocks',
                     '/merge_blocks',
                     '/faq']:
            with self.app.test_client() as c:
                rv = c.get(view)
                self.assertNotEqual(rv.status_code, 500)

    def test_leaderboard_anon(self):
        s = m.UserSettings(user="185cYTmEaTtKmBZc8aSGCr9v2VCDLqQHgR", anon=True)
        db.session.add(s)
        start = datetime.datetime.utcnow()
        now = start - datetime.timedelta(minutes=2)
        v = m.ShareSlice(time=now, value=101,
                         user="185cYTmEaTtKmBZc8aSGCr9v2VCDLqQHgR", worker="",
                         algo="scrypt", span=0, share_type="acc")
        db.session.add(v)
        v = m.ShareSlice(time=now, value=100,
                         user="DAbhwsnEq5TjtBP5j76TinhUqqLTktDAnD", worker="",
                         algo="scrypt", span=0, share_type="acc")
        db.session.add(v)
        db.session.commit()
        leaderboard()
        users = cache.get("leaderboard")
        self.assertEquals(users[0][0], "Anonymous")
        self.assertEquals(users[0][1]['scrypt'], 110318.93333333333)
        self.assertEquals(users[1][0], "DAbhwsnEq5TjtBP5j76TinhUqqLTktDAnD")
        self.assertEquals(users[1][1]['scrypt'], 109226.66666666667)


class TestViews(UnitTest):
    def test_cache_(self):
        s = m.UserSettings(user="185cYTmEaTtKmBZc8aSGCr9v2VCDLqQHgR", anon=True)
        db.session.add(s)
        db.session.commit()
        assert "185cYTmEaTtKmBZc8aSGCr9v2VCDLqQHgR" in anon_users()
