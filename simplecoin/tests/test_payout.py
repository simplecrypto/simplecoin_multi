import time
import flask
import unittest
import random
import decimal

from simplecoin import db, currencies
from simplecoin.scheduler import _distributor
from simplecoin.tests import RedisUnitTest, UnitTest
import simplecoin.models as m
from simplecoin.scheduler import (credit_block, create_payouts,
                                  generate_credits, create_trade_req,
                                  compress_slices)
from simplecoin.rpc_views import update_trade_requests

from itsdangerous import TimedSerializer
from decimal import Decimal


class TestDistributor(unittest.TestCase):
    def test_even_distrib(self):
        splits = {"a": Decimal(100)}
        _distributor(Decimal("100"), splits)
        assert splits["a"] == 100

    def test_basic_distrib(self):
        splits = {"a": Decimal(100),
                  "b": Decimal(256),
                  "c": Decimal(3)}
        amount = Decimal("100")

        _distributor(amount, splits)
        for k, val in splits.iteritems():
            assert isinstance(val, Decimal)

        assert splits["b"] > (splits["a"] * Decimal("2.5"))
        assert splits["a"] > (splits["c"] * Decimal("33.33333"))
        self.assertEquals(sum(splits.itervalues()), amount)

    def test_other(self):
        amount = Decimal("0.7109375")
        splits = {0: Decimal('0.9000000000000000000000000000'),
                  1: Decimal('0.1000000000000000000000000000')}

        ret = _distributor(amount, splits)
        self.assertEquals(sum(ret.itervalues()), amount)

    def test_final_hopefully(self):
        amount = Decimal("1.0117691900000000000000000000")
        splits = {"a": Decimal('0.0976562500000000000000000000'),
                  "b": Decimal('8.3125'),
                  "c": Decimal('0.8789062500000000000000000000'),
                  "d": Decimal('0.71484375')}
        _distributor(amount, splits)

    def test_prec_calc(self):
        amount = Decimal("1.4531250")
        splits = {0: Decimal('0.9000000000000000000000000000'),
                  1: Decimal('0.1000000000000000000000000000')}
        _distributor(amount, splits)

    def test_edge_case(self):
        amount = Decimal("1.00007884")
        splits = {"test": Decimal('0.32187500'),
                  "two": Decimal('2.89687500'),
                  "other": Decimal('2.78515625')}

        _distributor(amount, splits)


class TestGenerateTradeRequests(UnitTest):
    def test_payout_generation(self):
        blk1 = self.make_block(mature=True)
        blk2 = self.make_block(mature=False)
        tr = m.TradeRequest(
            quantity=sum(xrange(1, 20)),
            type="sell",
            currency="LTC"
        )

        pending = m.CreditExchange(
            amount="12.2394857987234598723453245",
            currency="DOGE",
            block=blk1,
            address="pending")
        db.session.add(pending)
        already_sold = m.CreditExchange(
            amount="12.2394857987234598723453245",
            currency="DOGE",
            block=blk1,
            sell_req=tr,
            address="already_sold")
        db.session.add(pending)
        immature = m.CreditExchange(
            amount="12.2394857987234598723453245",
            currency="DOGE",
            block=blk2,
            address="immature")
        db.session.add(immature)
        db.session.commit()

        create_trade_req("sell")
        db.session.rollback()

        trs = m.TradeRequest.query.all()

        assert pending.sell_req is not None
        assert pending.sell_req != already_sold.sell_req
        assert immature.sell_req is None
        assert already_sold.sell_req == tr
        assert len(trs) == 2
        assert trs[1].quantity == Decimal("12.2394857987234598723453245")


class TestGeneratePayout(UnitTest):
    def test_payout_generation(self):
        too_low = m.Credit(
            amount="0.0000000001",
            currency="DOGE",
            payable=True,
            address="too_low")
        db.session.add(too_low)
        grouped = m.Credit(
            amount="12.2394857987234598723453245",
            currency="DOGE",
            payable=True,
            address="grouped")
        db.session.add(grouped)
        double2 = m.Credit(
            amount="12",
            currency="DOGE",
            payable=True,
            address="double")
        db.session.add(double2)
        double1 = m.CreditExchange(
            buy_amount="12",
            currency="DOGE",
            payable=True,
            address="double")
        db.session.add(double1)

        db.session.commit()

        create_payouts()
        db.session.rollback()

        assert too_low.payout is None
        assert grouped.payout is not None
        remain = m.Credit.query.filter_by(address="grouped", payout=None).one()
        grouped_result = m.Payout.query.filter_by(address="grouped").one()
        double_result = m.Payout.query.filter_by(address="double").one()
        self.assertEquals(grouped_result.amount, Decimal("12.23948579"))
        self.assertEquals(remain.amount, Decimal('8.7234598723453245E-9'))
        self.assertEquals(grouped_result.amount + remain.amount,
                          grouped.amount)
        self.assertEquals(double_result.amount, double2.amount * 2)


class TestTradeRequest(UnitTest):
    def test_complete_tr_buy(self):
        credits = []
        tr = m.TradeRequest(
            quantity=sum(xrange(1, 20)),
            type="buy",
            currency="TEST"
        )
        db.session.add(tr)
        for i in xrange(1, 20):
            c = m.CreditExchange(
                amount=i,
                sell_amount=i,
                sell_req=None,
                buy_req=tr,
                currency="TEST",
                address="test{}".format(i))
            credits.append(c)
            db.session.add(c)

        db.session.commit()
        push_data = {'trs': {tr.id: {"status": 6, "quantity": "1000",
                                     "fees": "1", "stuck_quantity": "0"}}}
        db.session.expunge_all()

        with self.app.test_request_context('/?name=Peter'):
            flask.g.signer = TimedSerializer(self.app.config['rpc_signature'])
            flask.g.signed = push_data
            update_trade_requests()

        db.session.rollback()
        db.session.expunge_all()

        previous = 0
        for credit in m.CreditExchange.query.all():
            print credit.id, credit.sell_amount, credit.buy_amount
            assert credit.buy_amount > previous
            previous = credit.buy_amount

        assert m.TradeRequest.query.first()._status == 6

    def test_complete_tr_sell(self):
        credits = []
        tr = m.TradeRequest(
            quantity=sum(xrange(1, 20)),
            type="sell",
            currency="TEST"
        )
        db.session.add(tr)
        for i in xrange(1, 20):
            c = m.CreditExchange(
                amount=i,
                sell_req=tr,
                currency="TEST",
                address="test{}".format(i))
            credits.append(c)
            db.session.add(c)

        db.session.commit()
        push_data = {'trs': {tr.id: {"status": 6, "quantity": "1000",
                                     "fees": "1", "stuck_quantity": "0"}}}
        db.session.expunge_all()

        with self.app.test_request_context('/?name=Peter'):
            flask.g.signer = TimedSerializer(self.app.config['rpc_signature'])
            flask.g.signed = push_data
            update_trade_requests()

        db.session.rollback()
        db.session.expunge_all()

        previous = 0
        for credit in m.CreditExchange.query.all():
            print credit.id, credit.amount, credit.sell_amount
            assert credit.sell_amount > previous
            previous = credit.sell_amount

        assert m.TradeRequest.query.first()._status == 6

    def test_update_tr_buy(self):
        blk1 = self.make_block(mature=True)

        credits = []
        tr = m.TradeRequest(
            quantity=sum(xrange(1, 20)) / 2,
            type="buy",
            currency="TEST"
        )
        db.session.add(tr)
        for i in xrange(1, 20):
            amount = float(i) / 2
            c = m.CreditExchange(
                amount=amount,
                sell_amount=i,
                sell_req=None,
                buy_req=tr,
                currency="TEST",
                address="test{}".format(i),
                block=blk1)
            credits.append(c)
            db.session.add(c)

        db.session.commit()

        posted_payable_amt = 149
        posted_stuck_amt = 40
        posted_fees = 1
        push_data = {'trs': {tr.id: {"status": 5,
                                     "quantity": str(posted_payable_amt),
                                     "fees": str(posted_fees),
                                     "stuck_quantity": str(posted_stuck_amt)}}}
        db.session.expunge_all()

        with self.app.test_request_context('/?name=Peter'):
            flask.g.signer = TimedSerializer(self.app.config['rpc_signature'])
            flask.g.signed = push_data
            update_trade_requests()

        db.session.rollback()
        db.session.expunge_all()

        tr2 = m.TradeRequest.query.first()
        credits2 = m.CreditExchange.query.all()

        # Assert that the payable amount is 150
        with decimal.localcontext(decimal.BasicContext) as ctx:
            # ctx.traps[decimal.Inexact] = True
            ctx.prec = 100

            # Check config to see if we're charging exchange fees or not
            if self.app.config.get('cover_autoex_fees', False):
                posted_payable_amt += posted_fees

            # Assert that the the new payable amount is 150
            payable_amt = sum([credit.buy_amount for credit in credits2 if credit.payable is True])
            assert payable_amt == posted_payable_amt

            # Assert that the stuck amount is 40
            stuck_amt = sum([credit.buy_amount for credit in credits2 if credit.payable is False])
            assert stuck_amt == posted_stuck_amt

            # Check that the total BTC quantity represented hasn't changed
            btc_quant = sum([credit.amount for credit in credits2])
            assert btc_quant == tr2.quantity

            # Check that the old credits BTC amounts look sane
            old_btc_quant = sum([credit.amount for credit in credits2 if credit.payable is True])
            assert (old_btc_quant / tr2.avg_price) - posted_fees == posted_payable_amt

            # Check that the new credits BTC amounts look sane
            new_btc_quant = sum([credit.amount for credit in credits2 if credit.payable is False])
            assert new_btc_quant / tr2.avg_price == posted_stuck_amt

            # Check that new credit attrs are the same as old (fees, etc)
            for credit in credits2:
                if credit.payable is False:
                    assert credits2[credit.id - 20].fee_perc == credit.fee_perc
                    assert credits2[credit.id - 20].pd_perc == credit.pd_perc
                    assert credits2[credit.id - 20].block == credit.block
                    assert credits2[credit.id - 20].user == credit.user
                    assert credits2[credit.id - 20].sharechain_id == credit.sharechain_id
                    assert credits2[credit.id - 20].address == credit.address
                    assert credits2[credit.id - 20].currency == credit.currency
                    assert credits2[credit.id - 20].type == credit.type
                    assert credits2[credit.id - 20].payout == credit.payout

        # Assert that the status is 5, partially completed
        assert tr2._status == 5

    def test_close_after_update_tr_buy(self):

        self.test_update_tr_buy()

        tr = m.TradeRequest.query.first()

        # Check that another update can be pushed.
        posted_payable_amt = 188.5
        posted_stuck_amt = 0
        posted_fees = 1.5
        push_data = {'trs': {tr.id: {"status": 6,
                                     "quantity": str(posted_payable_amt),
                                     "fees": str(posted_fees),
                                     "stuck_quantity": str(posted_stuck_amt)}}}
        db.session.expunge_all()

        with self.app.test_request_context('/?name=Peter'):
            flask.g.signer = TimedSerializer(self.app.config['rpc_signature'])
            flask.g.signed = push_data
            update_trade_requests()

        db.session.rollback()
        db.session.expunge_all()

        tr2 = m.TradeRequest.query.first()
        credits2 = m.CreditExchange.query.all()

        # Assert that the payable amount is 150
        with decimal.localcontext(decimal.BasicContext) as ctx:
            # ctx.traps[decimal.Inexact] = True
            ctx.prec = 100

            # Check config to see if we're charging exchange fees or not
            if self.app.config.get('cover_autoex_fees', False):
                posted_payable_amt += posted_fees

            # Assert that the the total payable amount is what we posted
            payable_amt = sum([credit.buy_amount for credit in credits2 if credit.payable is True])
            assert payable_amt == posted_payable_amt
            total_curr_quant = sum([credit.buy_amount for credit in credits2])
            assert total_curr_quant + Decimal(posted_fees) == 190

            # Check that the total BTC quantity equals out
            btc_quant = sum([credit.amount for credit in credits2])
            assert btc_quant == tr2.quantity

            # Check that the BTC amounts look sane
            btc_quant = sum([credit.amount for credit in credits2 if credit.payable is True])
            assert (btc_quant / tr2.avg_price) - Decimal(posted_fees) == posted_payable_amt

            # Check that there is nothing marked unpayable
            not_payable = sum([credit.amount for credit in credits2 if credit.payable is False])
            assert not not_payable


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

    def random_shares(self):
        """ Generate some random shares. Somewhat bad practice for unit tests.
        Perhaps we set a static randomness seed in the future """
        addresses = ['DSAEhYmKZmDN9e1vGPRWSvRQEiWGARhiVh',
                     'DLePZigvzzvSyoWztctVVsPtDuhzBfqEgd',
                     'LVsJCXPThJzGhenQT2yuAEy82RTDQjQUYy',
                     'DKcNvReNSfaCV9iCJjBnxt8zJfiTqzv2vk',
                     'D6xxcZtoQuCajFgVaoPgsq31WNHFst3yce']
        lst = []
        for i in xrange(250):
            lst.append("{}:{}".format(random.choice(addresses),
                                      random.randint(1, 200)))
        return lst

    def test_slice_compression(self):
        """ Make sure that uncompressed and compressed slices behave the same
        for paying out users """
        shares = self.random_shares()

        def setup():
            self.app.redis.hmset(
                "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
                self.test_block_data)
            self.app.redis.rpush("chain_1_slice", "DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF:1")
            self.app.redis.set("chain_1_slice_index", 17)
            self.app.redis.rpush("chain_1_slice_17", *shares)

        setup()
        credit_block("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")
        payouts = [(p.address, p.amount, p.currency, p.user) for p in
                   m.Credit.query.order_by(m.Credit.user)]

        # reset everything, and try it compressed
        self.app.redis.flushdb()
        self.tearDown()
        self.setup_db()

        setup()
        compress_slices()
        assert self.app.redis.type("chain_1_slice_17") == "hash"
        credit_block("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")
        payouts_compress = [(p.address, p.amount, p.currency, p.user) for p in
                            m.Credit.query.order_by(m.Credit.user)]
        self.assertEqual(payouts_compress, payouts)

    def test_payout_unexchangeable(self):
        """ Payout an unexchangeable currency """
        bd = self.test_block_data.copy()
        bd.update(dict(currency="TCO"))
        self.app.redis.hmset(
            "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
            bd)

        self.app.redis.rpush("chain_1_slice_17", *["DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF:1"] * 30)
        self.app.redis.rpush("chain_1_slice_17", *["DLePZigvzzvSyoWztctVVsPtDuhzBfqEgd:1"] * 30)
        user_set = m.UserSettings(user="DLePZigvzzvSyoWztctVVsPtDuhzBfqEgd")
        addr = m.PayoutAddress(user='DLePZigvzzvSyoWztctVVsPtDuhzBfqEgd',
                               address='LbfSCZE1p9A3Yj2JK1n57kxyD2H1ZSXtNG',
                               currency='TCO')
        db.session.add(user_set)
        db.session.add(addr)
        db.session.commit()

        credit_block("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")

        db.session.rollback()
        db.session.expunge_all()
        payouts = m.Credit.query.all()
        for p in payouts:
            print((p.id, p.user, p.address, p.block, p.currency, p.block.currency, p.source))
        pool_payouts = m.Credit.query.filter_by(address=currencies['TCO'].pool_payout_addr).all()
        m.Credit.query.filter_by(address='LbfSCZE1p9A3Yj2JK1n57kxyD2H1ZSXtNG').one()
        self.assertEqual(len(payouts), 3)
        self.assertEqual(len(pool_payouts), 2)

    def test_payout_multichain(self, **kwargs):
        """ Try paying out users from multiple chains """
        bd = self.test_block_data.copy()
        bd.update(dict(chain_2_solve_index="1", chain_2_shares="18"))
        bd.update(**kwargs)
        self.app.redis.hmset(
            "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
            bd)
        self.app.redis.rpush("chain_1_slice_17", *["DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF:1"] * 30)
        self.app.redis.rpush("chain_2_slice_1", *["testing:1"] * 32)

        credit_block("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")
        pool_payout = currencies[self.app.config['pool_payout_currency']]

        db.session.rollback()
        db.session.expunge_all()
        payouts = m.Credit.query.all()
        self.assertEqual(len(payouts), 3)
        block = m.Block.query.first()
        self.assertEqual(len(block.chain_payouts), 2)
        self.assertEqual(block.chain_payouts[0].amount, block.total_value / 2)
        self.assertEqual(block.chain_payouts[1].amount, block.total_value / 2)
        self.assertEqual(payouts[1].address, pool_payout.pool_payout_addr)
        for p in payouts:
            print p.id, p.currency, p.block.currency, p.type
            assert p.block == block
            if p.block.currency == p.currency:
                assert p.type == 0
            else:
                assert p.type == 1

        assert m.Credit.query.filter_by(source=1).one().amount == Decimal("0.250")

    def test_payout(self, **kwargs):
        """ A regular, basic payout to one chain, with one user """
        bd = self.test_block_data.copy()
        bd.update(**kwargs)
        self.app.redis.hmset(
            "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
            bd)

        self.app.redis.rpush("chain_1_slice_17", *["DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF:1"] * 30)
        credit_block("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")

        db.session.rollback()
        db.session.expunge_all()
        payouts = m.Credit.query.all()
        self.assertEqual(len(payouts), 2)
        block = m.Block.query.first()
        self.assertEqual(block.currency, self.test_block_data['currency'])
        self.assertEqual(block.total_value, Decimal("50"))
        self.assertEqual(block.height, 247)
        self.assertEqual(len(block.chain_payouts), 1)
        self.assertEqual(block.chain_payouts[0].amount, block.total_value)
        for p in payouts:
            print p.id, p.currency, p.block.currency, p.type
            assert p.block == block
            if p.block.currency == p.currency:
                assert p.type == 0
            else:
                assert p.type == 1

        assert m.Credit.query.filter_by(source=1).one().amount == Decimal("0.50")

    def test_payout_donation(self):
        """ Make sure users setting donation percentages work properly """
        s = m.UserSettings(user="DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF",
                           pdonation_perc=Decimal("0.05"))
        db.session.add(s)
        self.app.redis.hmset(
            "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
            self.test_block_data.copy())

        self.app.redis.rpush("chain_1_slice_17", *["DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF:1"] * 30)
        credit_block("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")

        db.session.rollback()
        db.session.expunge_all()

        assert m.Credit.query.filter_by(user="DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF").count() == 1
        donation = m.Credit.query.filter_by(source=2).one()
        assert donation.amount < 3

    def test_payout_special_split(self):
        """ Make sure that special payout splits work as expected """
        s = m.UserSettings(user="DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF",
                           spayout_perc=Decimal("0.05"),
                           spayout_addr="DAbhwsnEq5TjtBP5j76TinhUqqLTktDAnD",
                           spayout_curr="DOGE")
        db.session.add(s)
        self.app.redis.hmset(
            "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
            self.test_block_data.copy())

        self.app.redis.rpush(
            "chain_1_slice_17", *["DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF:1"] * 30)
        credit_block("unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f")

        db.session.rollback()
        db.session.expunge_all()

        assert m.Credit.query.filter_by(
            user="DJCgMCyjBKxok3eEGed5SGhbWaGj5QTcxF").count() == 2
        split = m.Credit.query.filter_by(
            address="DAbhwsnEq5TjtBP5j76TinhUqqLTktDAnD").one()
        assert split.amount < 3

    def test_payout_merged(self):
        """ Make sure that paying out merged block works properly """
        self.test_payout(merged="1")
        assert m.Block.query.first().merged

    def test_payout_multichain_merged(self):
        """ Make sure that paying out multiple chains still sets merged
        properly """
        self.test_payout_multichain(merged="1")
        assert m.Block.query.first().merged

    def test_generate_zero(self):
        """ When a user sets a 100% split payout address there original credit
        gets created with 0 output. Make sure that this still works. """
        chain_shares = {1: {'D5nYTCs9aNg5QAcw35KZj45ZA9iFbN6ZU7': Decimal('381120')}}
        self.app.redis.rpush(
            "chain_1_slice_25",
            *["{}:{}".format(user, shares) for user, shares in chain_shares[1].iteritems()])

        self.app.redis.hmset(
            "unproc_block_01c5da46e845868a7ead5eb97d07c4299b6370e65fd4313416772e181c0c756f",
            {'chain_1_solve_index': '45',
             'hash': '0dbc436b29e577e7932dd13655179f3c3e06b2940be4e0e04f87069caa0f603f',
             'total_subsidy': '13517762478',
             'start_time': '1410401945.063338',
             'address': 'DMNvCJ33EBQn14S1hb1chBk1XoiBEZ4ScJ',
             'worker': 'worker1',
             'height': '35630',
             'currency': 'SYS',
             'algo': 'scrypt',
             'fees': '-1',
             'chain_1_shares': '56583488',
             'hex_bits': '1C008FA7',
             'solve_time': '1410412589.175215',
             'merged': '1'})

        s = m.UserSettings(user="D5nYTCs9aNg5QAcw35KZj45ZA9iFbN6ZU7",
                           spayout_perc=Decimal("1.00"),
                           spayout_addr="1JBDMJWBYgA6Rmp8EUPyzFQp79uNsJb67R",
                           spayout_curr="BTC",
                           pdonation_perc=Decimal("0.05"))
        db.session.add(s)
        db.session.commit()
        db.session.expunge_all()

        generate_credits()
        db.session.rollback()


if __name__ == "__main__":
    import sys
    t = time.time()
    for i in xrange(10000):
        amount = Decimal(str(random.uniform(1, 100000)))
        splits = {}
        for i in xrange(random.randint(1, 5)):
            splits[i] = Decimal(str(random.uniform(1, 100)))
            round_sz = Decimal((0, (1, ), -1 * random.randint(2, 15)))
            splits[i] = splits[i].quantize(round_sz)
        sys.stdout.write(".")
        _distributor(amount, splits)
    print "completed in {}".format(time.time() - t)
