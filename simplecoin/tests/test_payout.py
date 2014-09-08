import unittest

from decimal import Decimal
from simplecoin import db, currencies
from simplecoin.scheduler import distributor
from simplecoin.tests import RedisUnitTest, UnitTest
from simplecoin.models import Credit, Block
from simplecoin.scheduler import payout


class TestDistributor(unittest.TestCase):
    def test_even_distrib(self):
        splits = {"a": Decimal(100)}
        distributor(Decimal("100"), splits)
        assert splits["a"] == 100

    def test_basic_distrib(self):
        splits = {"a": Decimal(100),
                  "b": Decimal(256),
                  "c": Decimal(3)}

        distributor(Decimal("100"), splits)
        for k, val in splits.iteritems():
            assert isinstance(val, Decimal)

        assert splits["b"] > (splits["a"] * Decimal("2.5"))
        assert splits["a"] > (splits["c"] * Decimal("33.33333"))


class TestPayouts(RedisUnitTest):
    test_block_data = {
        "start_time": "1408865090.230471",
        "chain_1_shares": "18",
        "solve_time": "1408865115.477793",
        "hash": "01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
        "fees": "0",
        "worker": "testing",
        "height": "247",
        "currency": "DOGE",
        "algo": "scrypt",
        "address": "Vfmiz3ZVZfXFvpZTtsLnvHXJCRmsZiaVFH",
        "total_subsidy": "5000000000",
        "hex_bits": "1e0ffff0",
        "chain_1_solve_index": "17",
        "merged": "0"
    }

    def test_payout_multichain(self, **kwargs):
        bd = self.test_block_data.copy()
        bd.update(dict(chain_2_solve_index="1", chain_2_shares="18"))
        bd.update(**kwargs)
        self.app.redis.hmset(
            "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
            bd)
        self.app.redis.rpush("chain_1_slice_17", *["DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF:1"] * 30)
        self.app.redis.rpush("chain_2_slice_1", *["testing:1"] * 32)

        payout("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")
        pool_payout = currencies[self.app.config['pool_payout_currency']]

        db.session.rollback()
        db.session.expunge_all()
        payouts = Credit.query.all()
        self.assertEqual(len(payouts), 3)
        block = Block.query.first()
        self.assertEqual(len(block.chain_payouts), 2)
        self.assertEqual(block.chain_payouts[0].amount, block.total_value / 2)
        self.assertEqual(block.chain_payouts[1].amount, block.total_value / 2)
        self.assertEqual(payouts[1].address, pool_payout.pool_payout_addr)
        for p in payouts:
            assert p.block == block

        assert Credit.query.filter_by(source=1).one().amount == Decimal("0.250")

    def test_payout(self, **kwargs):
        bd = self.test_block_data.copy()
        bd.update(**kwargs)
        self.app.redis.hmset(
            "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
            bd)

        self.app.redis.rpush("chain_1_slice_17", *["DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF:1"] * 30)
        payout("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")

        db.session.rollback()
        db.session.expunge_all()
        payouts = Credit.query.all()
        self.assertEqual(len(payouts), 2)
        block = Block.query.first()
        self.assertEqual(block.currency, self.test_block_data['currency'])
        self.assertEqual(block.total_value, Decimal("50"))
        self.assertEqual(block.height, 247)
        self.assertEqual(len(block.chain_payouts), 1)
        self.assertEqual(block.chain_payouts[0].amount, block.total_value)
        for p in payouts:
            assert p.block == block

        assert Credit.query.filter_by(source=1).one().amount == Decimal("0.50")

    def test_payout_merged(self):
        self.test_payout(merged="1")
        assert Block.query.first().merged

    def test_payout_multichain_merged(self):
        self.test_payout_multichain(merged="1")
        assert Block.query.first().merged
