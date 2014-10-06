from simplecoin import currencies
from simplecoin.exceptions import InvalidAddressException
from simplecoin.tests import UnitTest


class TestConfig(UnitTest):
    def test_config_eq(self):
        self.assertNotEqual(currencies['TCO'], currencies['DOGE'])
        self.assertEqual(currencies['TCO'], currencies['TCO'])
        assert currencies['TCO'] > currencies['DOGE']
        assert currencies['DOGE'] < currencies['TCO']
        assert None not in currencies.values()

    def test_lookup_payable(self):
        currencies.lookup_payable_addr("185cYTmEaTtKmBZc8aSGCr9v2VCDLqQHgR")
        currencies.lookup_payable_addr("DAbhwsnEq5TjtBP5j76TinhUqqLTktDAnD")
        self.assertRaises(
            InvalidAddressException,
            lambda: currencies.lookup_payable_addr(
                "VkbHY8ua2TjxdL7gY2uMfCz3TxMzMPgmRR"))
        self.assertRaises(
            InvalidAddressException,
            lambda: currencies.lookup_payable_addr(
                "test"))
