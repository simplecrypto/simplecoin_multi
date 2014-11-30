from simplecoin import cache, chains
from simplecoin.scheduler import chain_cleanup
from simplecoin.tests import RedisUnitTest

import random


class TestTasks(RedisUnitTest):
    def test_share_slice_cleanup(self):
        # Make 10 or so fake compressed share slices. Count the shares for half of them
        total_shares = 0
        for i in xrange(10):
            shares = random.randint(1, 200)
            if i <= 5:
                total_shares += shares
            self.app.redis.hmset("chain_1_slice_{}".format(i), dict(total_shares=shares))
        self.app.redis.set("chain_1_slice_index", 9)

        # Find a fake difficulty that would cause deletion of one half of the
        # share slices
        hashes_for_shares = total_shares * float(chains[1].algo.hashes_per_share)
        diff_for_shares = hashes_for_shares / (2 ** 32)
        # Safety_margin + last_n multiplies by 4
        diff_for_shares /= 4
        cache.set("DOGE_data", dict(difficulty_avg=diff_for_shares,
                                    difficulty_avg_stale=False, timeout=1200))

        chain_cleanup(chains[1], dont_simulate=True)
        # 11 total keys, we will delete 5
        self.assertEquals(len(self.app.redis.keys("chain_1_slice_*")), 6)
