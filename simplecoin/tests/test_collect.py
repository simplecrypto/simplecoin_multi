from simplecoin import db, global_config
from simplecoin.tests import RedisUnitTest
from simplecoin.models import ShareSlice, DeviceSlice
from simplecoin.scheduler import collect_minutes, collect_ppagent_data


class TestCollectMinutes(RedisUnitTest):
    test_block_data = {
        "pool.": "2.4",
        "donate.": "2.4"
    }

    def test_collect(self, **kwargs):
        bd = self.test_block_data.copy()
        bd.update(**kwargs)
        self.app.redis.hmset("min_acc_scrypt_1409899740", bd)

        collect_minutes()

        db.session.rollback()
        db.session.expunge_all()
        sl_pool = ShareSlice.query.filter_by(user="pool").one()
        sl_donate = ShareSlice.query.filter_by(
            user=global_config.pool_payout_currency.pool_payout_addr).one()
        assert sl_pool.value == 2.4
        assert sl_pool.share_type == "acc"
        assert sl_donate.value == 2.4
        assert sl_donate.share_type == "acc"

    def test_collect_ppagent(self, **kwargs):
        self.app.redis.hmset("hashrate_1409899740", dict(test__0="None", test__1="12.5"))

        collect_ppagent_data()

        db.session.rollback()
        db.session.expunge_all()
        sl = DeviceSlice.query.all()
        assert sl[0].user == "test"
        assert sl[0].value == 12.5 * 1000000
