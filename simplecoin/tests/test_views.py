import datetime

from simplecoin import db, cache
from simplecoin.scheduler import leaderboard
from simplecoin.tests import RedisUnitTest
import simplecoin.models as m


class TestViews(RedisUnitTest):
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
