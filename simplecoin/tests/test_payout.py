import unittest

from decimal import Decimal
from simplecoin.scheduler import distributor


class TestDistributor(unittest.TestCase):
    def test_basic_distrib(self):
        splits = {"a": Decimal(100),
                  "b": Decimal(256),
                  "c": Decimal(3)}

        distributor(Decimal("100"), splits)
        for k, val in splits.iteritems():
            assert isinstance(val, Decimal)

        assert splits["b"] > (splits["a"] * Decimal("2.5"))
        assert splits["a"] > (splits["c"] * Decimal("33.33333"))
