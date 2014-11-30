import simplecoin
import unittest
import datetime
import random

import simplecoin.models as m

from decimal import Decimal
from simplecoin import db


class UnitTest(unittest.TestCase):
    """ Represents a set of tests that only need the database iniailized, but
    no fixture data """
    def setUp(self, **kwargs):
        # Set the random seed to a fixed number, causing all use of random
        # to actually repeat exactly the same every time
        random.seed(0)
        extra = dict()
        extra.update(kwargs)
        app = simplecoin.create_app('webserver', configs=['test.toml'], **extra)
        with app.app_context():
            self.db = simplecoin.db
            self.setup_db()

        self.app = app
        self._ctx = self.app.test_request_context()
        self._ctx.push()
        self.client = self.app.test_client()

    def tearDown(self):
        # dump the test elasticsearch index
        db.session.remove()
        db.drop_all()

    def setup_db(self):
        self.db.drop_all()
        self.db.create_all()
        db.session.commit()

    def make_block(self, **kwargs):
        vals = dict(currency="LTC",
                    height=1,
                    found_at=datetime.datetime.utcnow(),
                    time_started=datetime.datetime.utcnow(),
                    difficulty=12,
                    merged=False,
                    algo="scrypt",
                    total_value=Decimal("50"))
        vals.update(kwargs)
        blk = m.Block(**vals)
        db.session.add(blk)
        return blk

    def random_shares(self):
        addresses = ['DSAEhYmKZmDN9e1vGPRWSvRQEiWGARhiVh',
                     'DLePZigvzzvSyoWztctVVsPtDuhzBfqEgd',
                     'LVsJCXPThJzGhenQT2yuAEy82RTDQjQUYy',
                     'DKcNvReNSfaCV9iCJjBnxt8zJfiTqzv2vk',
                     'D6xxcZtoQuCajFgVaoPgsq31WNHFst3yce']
        lst = []
        total = 0
        for i in xrange(250):
            shares = random.randint(1, 200)
            lst.append("{}:{}".format(random.choice(addresses), shares))
            total += shares
        return lst, total


class RedisUnitTest(UnitTest):
    def setUp(self):
        UnitTest.setUp(self)
        self.app.redis.flushdb()
