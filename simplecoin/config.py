import requests

from flask import current_app
from cryptokit.rpc import CoinserverRPC, CoinRPCException
from cryptokit.base58 import address_version
from decimal import Decimal as dec
from urlparse import urljoin

from . import models as m
from . import cache, redis_conn, currencies, exchanges, chains, powerpools, locations
from .exceptions import ConfigurationException, RemoteException


class ConfigChecker(object):
    """
    This class provides various methods for validating config values and checks
    configuration values and makes sure they're properly filled out.

    It needs a lot of expansion :/

    Currently validates the following keys:
    pool_payout_address
    currencies
    """

    def __init__(self, config):
        self.config = config

    def lookup_key(self, key, nested=None):
        """ Helper method: Checks the config for the specified key and raises
        an error if not found """

        try:
            if not nested:
                value = self.config[key]
            else:
                value = nested[key]
        except KeyError:
            raise ConfigurationException('Failed when looking up \'{}\' in the '
                                         'config. This key is required!'.format(key))
        else:
            return value

    def check_truthiness(self, val):
        """ Helper method: Checks a value for truthiness """
        if not val:
            raise ConfigurationException('Value \'{}\' is not truthy. This '
                                         'value is required!'.format(val))

    def check_type(self, val, obj_type):
        """ Helper method: Checks a value to make sure its the correct type """
        if not type(val) is obj_type:
            raise ConfigurationException("\'{}\' is not an instance of {}".format(val))

    def check_is_bcaddress(self, val):
        """ Helper method: Checks a value for truthiness """
        try:
            ver = address_version(val)
        except (KeyError, AttributeError):
            raise ConfigurationException("\'{}\' is not a valid bitcoin style "
                                         "address".format(val))
        return ver

    def parse_config(self):
        """ Go through config keys and perform the appropriate logic checks """

        # Check the GLOBAL 'pool_payout_address field'
        p_addr = self.lookup_key('pool_payout_address')
        self.check_truthiness(p_addr)
        self.check_is_bcaddress(p_addr)

        # Check the 'currencies' key
        currencies = self.lookup_key('currencies')
        self.check_truthiness(currencies)
        self.check_type(currencies, dict)
        for curr_key, curr_cfg in currencies.iteritems():
            c_addr = self.lookup_key('pool_payout_addr', nested=currencies[curr_key])
            c_ver = self.lookup_key('address_version', nested=currencies[curr_key])
            self.check_type(c_ver, list)
            exchangeable = self.lookup_key('exchangeable', nested=currencies[curr_key])
            # If a pool payout addr is specified, make sure it matches the
            # configured address version.
            if c_addr:
                ver = CurrencyKeeper.check_is_bcaddress(c_addr)
                if ver not in c_ver:
                    raise ConfigurationException("{} is not a valid {} address."
                                                 " Must be {}"
                                                 .format(c_addr, curr_key, c_ver))
            # Check for valid  pool address for unexchangeable currencies
            if not exchangeable and not c_addr:
                raise ConfigurationException(
                    "Unexchangeable currencies require a pool payout addr."
                    "No valid address found for {}".format(curr_key))


class ConfigObject(object):
    __getitem__ = lambda self, a: getattr(self, a)
    requires = []
    defaults = dict()

    def __init__(self, bootstrap):
        for req in self.requires:
            if req not in bootstrap:
                raise ConfigurationException(
                    "{} item requires {}".format(self.__class__.__name__, req))
        # Default settings
        self.__dict__.update(self.defaults)
        self.__dict__.update(bootstrap)

    def __str__(self):
        return self.key

    def __repr__(self):
        return self.key

    def __hash__(self):
        return hash(self.key)


class Currency(ConfigObject):
    requires = ['algo', 'name', 'coinserv', 'address_version',
                'trans_confirmations', 'block_time', 'block_mature_confirms']
    defaults = dict(exchangeable=False,
                    pool_payout_addr=None)

    def __init__(self, bootstrap):
        ConfigObject.__init__(self, bootstrap)
        self.coinserv = CoinserverRPC(
            "http://{0}:{1}@{2}:{3}/"
            .format(bootstrap['coinserv']['username'],
                    bootstrap['coinserv']['password'],
                    bootstrap['coinserv']['address'],
                    bootstrap['coinserv']['port'],
                    pool_kwargs=dict(maxsize=bootstrap.get('maxsize', 10))))

    @property
    @cache.memoize(timeout=3600)
    def btc_value(self):
        """ Caches and returns estimated currency value in BTC """
        if self.key == "BTC":
            return dec('1')

        # XXX: Needs better number here!
        err, dat, _ = exchanges.optimal_sell(self.key, dec('1000'), exchanges._get_current_object().exchanges)
        try:
            current_app.logger.info("Got new average price of {} for {}"
                                    .format(dat['avg_price'], self))
            return dat['avg_price']
        except (KeyError, TypeError):
            current_app.logger.warning("Unable to grab price for currency {}, got {} from autoex!"
                                       .format(self.key, dict(err=err, dat=dat)))
            return dec('0')

    def est_value(self, other_currency, amount):
        val = self.btc_value
        if val:
            return amount * val / other_currency.btc_value
        return dec('0')

    def __repr__(self):
        return self.key
    __str__ = __repr__

    def __hash__(self):
        return self.key.__hash__()


class CurrencyKeeper(dict):
    __getattr__ = dict.__getitem__

    def __init__(self, currency_dictionary):
        super(CurrencyKeeper, self).__init__()
        for key, config in currency_dictionary.iteritems():
            config['key'] = key
            obj = Currency(config)
            if key in self:
                raise ConfigurationException("Duplicate currency keys {}"
                                             .format(key))
            self[obj.key] = obj


    @property
    def exchangeable_currencies(self):
        return [c for c in self.itervalues() if c.exchangeable is True]

    @property
    def unexchangeable_currencies(self):
        return [c for c in self.itervalues() if c.exchangeable is False]

    @property
    def available_currencies(self):
        return [k for k in self.iterkeys()]

    @property
    def available_versions(self):
        versions = {}
        for v in self.itervalues():
            for version in v.address_version:
                versions.setdefault(version, [])
                versions[version].append(v)
        return versions

    def lookup_address(self, address):
        ver = address_version(address)
        try:
            return self.lookup_version(ver)
        except AttributeError:
            raise AttributeError("Address '{}' version {} is not a configured "
                                 "currency. Options are {}"
                                 .format(address, ver, self.available_versions))

    def lookup_version(self, version):
        try:
            return self.available_versions[version]
        except KeyError:
            raise AttributeError(
                "Address version {} doesn't match available versions {}"
                .format(version, self.available_versions))

    def lookup_payable_addr(self, address):
        """
        Checks an address to determine if its a valid and payable(exchangeable)
        address. Typically used to validate a username address.
        Returns the payable currency object for that version.

        When calling this function you should always expect exceptions to be
        raised.

        !!! This function assumes that a currency will not be configured as
        exchangeable if there is a version conflict with another currency.

        Although it makes this assumption - it should return a consistent
        Currency obj even if configuration is incorrect
        """
        curr = self.validate_bc_address(address)

        for currency in curr:
            if currency in currencies.exchangeable_currencies:
                return currency
            else:
                raise AttributeError("Address '{}' version {} is not an "
                                     "exchangeable currency. Options are {}"
                                     .format(address, curr.address_version,
                                             self.exchangeable_currencies))

    def validate_bc_address(self, bc_address_str):
        """
        The go-to function for all your bitcoin style address validation needs.

        Expects to receive a string believed to represent a bitcoin address
        Raises appropriate errors if any checks are failed, otherwise returns a
        list of Currency objects that have the same addr version.
        """
        # First check to make sure the address contains only alphanumeric chars
        if not bc_address_str.isalnum():
            raise TypeError('Address should be alphanumeric')

        # Check to make sure str is the proper length
        if not len(bc_address_str) >= 33 or not len(bc_address_str) <= 35:
            raise ValueError('Address should be 33-35 characters long')

        # Check to see if the address can be looked up from the config
        curr = currencies.lookup_address(bc_address_str)
        # If we've arrived here we'll consider it valid
        return curr


class Chain(ConfigObject):
    requires = ['type', 'valid_address_versions']
    defaults = dict(block_bonus="0")
    max_indexes = 1000
    min_index = 0

    def __init__(self, bootstrap):
        ConfigObject.__init__(self, bootstrap)
        # Check all our valid versions and ensure we have configuration
        # information on them

        assert isinstance(self.fee_perc, basestring)
        assert isinstance(self.block_bonus, basestring)
        self.fee_perc = dec(self.fee_perc)
        self.hr_fee_perc = round(self.fee_perc * 100, 2)

    def valid_address(self, address):
        """ Is the given address valid for this payout chain? """
        try:
            if currencies.lookup_address(address) not in self.valid_address_versions:
                return False
        except AttributeError:
            return False

        return True

    def __hash__(self):
        return self.id

    def calc_shares(self, block_payout):
        """ Pass a block_payout object with only chain ID and blockhash
        populated and compute share amounts """
        raise NotImplementedError

    def _calc_shares(self, start_slice, target_shares=None, stop_slice=None):
        if target_shares == 0:
            raise ValueError("Taget shares must be positive")
        if target_shares is None and stop_slice is None:
            raise ValueError("Must define either a stop slice or oldest valid slice.")

        current_app.logger.info("Calculating share count with start_slice {}; stop slice {}; target_shares {}"
                                .format(start_slice, stop_slice, target_shares))

        # We want to iterate backwards through the slices until we've collected
        # the target shares, or reached the stop slice.

        # The oldest slice we want to look at is either the minimum index number,
        # or start slice minus max_indexes...
        new_stop_slice = max(self.min_index, start_slice - self.max_indexes, stop_slice)
        current_app.logger.debug("Out of min_index ({}), start_slice - max_indexes "
                                 "({}), and stop_index ({}) {} was used"
                                 .format(self.min_index, start_slice -
                                         self.max_indexes, stop_slice,
                                         new_stop_slice))
        stop_slice = new_stop_slice
        if stop_slice > start_slice:
            raise Exception("stop_slice {} cannot be greater than start_slice {}!"
                            .format(stop_slice, start_slice))

        found_shares = 0
        users = {}
        index = 0
        for index in xrange(start_slice, stop_slice, -1):
            slc = "chain_{}_slice_{}".format(self.id, index)
            entries = redis_conn.lrange(slc, 0, -1)
            for entry in entries:
                user, shares = entry.split(":")
                if user not in users:
                    users[user] = dec(shares)
                else:
                    users[user] += dec(shares)
                found_shares += float(shares)
                if target_shares and found_shares >= target_shares:
                    break

            if target_shares and found_shares >= target_shares:
                break

        current_app.logger.info(
            "Collected and counted {:,} shares for {:,} users from slice #{:,} -> #{:,}."
            .format(found_shares, len(users), start_slice, index))

        return users


class PPLNSChain(Chain):
    requires = Chain.requires[:]
    requires.extend(['last_n'])

    def __init__(self, bootstrap):
        Chain.__init__(self, bootstrap)
        self.last_n = float(self.last_n)

    def calc_shares(self, block_payout):
        assert block_payout.chainid == self.id
        target_shares = int(round(block_payout.block.difficulty * (2 ** 16) * self.last_n))
        return self._calc_shares(block_payout.solve_slice, target_shares=target_shares)


class PropChain(Chain):
    def calc_shares(self, block_payout):
        assert block_payout.chainid == self.id
        curr_block = block_payout.block
        last_block = (m.Block.query.filter_by(algo=curr_block.algo,
                                              merged=curr_block.merged,
                                              currency=curr_block.currency).
                      filter(m.Block.hash != curr_block.hash).
                      order_by(m.Block.found_at.desc())).first()
        if last_block:
            last_block_payout = [bp for bp in last_block.block_payouts if bp.chainid == self.id][0]
            stop_slice = last_block_payout.solve_slice
        else:
            stop_slice = 0
        return self._calc_shares(block_payout.solve_slice, stop_slice=stop_slice)


class ChainKeeper(dict):
    type_map = {"pplns": PPLNSChain,
                "prop": PropChain}

    def __init__(self, configs):
        super(ChainKeeper, self).__init__()
        defaults = configs.pop('defaults', {})
        for id, cfg in configs.iteritems():
            pass_cfg = defaults.copy()
            pass_cfg['id'] = id
            pass_cfg.update(cfg)
            serv = self.type_map[cfg['type']](pass_cfg)
            self[id] = serv


class Location(ConfigObject):
    required = ['location_acronym', 'location', 'country_flag']

    def stratums_by_algo(self):
        by_algo = {}
        for strat in powerpools.itervalues():
            if strat._location == self.key:
                lst = by_algo.setdefault(strat.chain.algo, [])
                lst.append(strat)
        return by_algo


class LocationKeeper(dict):
    def __init__(self, configs):
        super(LocationKeeper, self).__init__()
        for key, cfg in configs.iteritems():
            cfg['key'] = key
            loc = Location(cfg)
            self[key] = loc


class Algo(ConfigObject):
    defaults = dict(enabled=True)


class AlgoKeeper(dict):
    def __init__(self, configs):
        super(AlgoKeeper, self).__init__()
        for algo, cfg in configs.iteritems():
            cfg['key'] = algo
            serv = Algo(cfg)
            self[algo] = serv

    def active_algos(self):
        return [a for a in self.itervalues() if a.enabled]


class PowerPool(ConfigObject):
    timeout = 10
    requires = ['_chain', 'port', 'address', 'monitor_address', 'unique_id',
                '_location']

    def __init__(self, bootstrap):
        bootstrap['_chain'] = bootstrap.pop('chain')
        bootstrap['_location'] = bootstrap.pop('location')
        ConfigObject.__init__(self, bootstrap)

    @property
    def stratum_address(self):
        return "stratum+tcp://{}:{}".format(self.address, self.port)
    display_text = stratum_address

    __repr__ = lambda self: self.monitor_address
    __str__ = lambda self: self.monitor_address

    def __hash__(self):
        return self.unique_id

    def request(self, url, method='GET', max_age=None, signed=True, **kwargs):
        url = urljoin(self.monitor_address, url)
        ret = requests.request(method, url, timeout=self.timeout, **kwargs)
        if ret.status_code != 200:
            raise RemoteException("Non 200 from remote: {}".format(ret.text))

        current_app.logger.debug("Got {} from remote".format(ret.text.encode('utf8')))
        return ret.json()

    @property
    def location(self):
        return locations[self._location]

    @property
    def chain(self):
        return chains[self._chain]


class PowerPoolKeeper(dict):
    def __init__(self, mining_servers):
        super(PowerPoolKeeper, self).__init__()
        self.stratums = []
        for id, cfg in mining_servers.iteritems():
            cfg['unique_id'] = id
            serv = PowerPool(cfg)

            # Setup all the child stratum objects
            serv.stratums = []
            if serv.unique_id in self:
                raise ConfigurationException("You cannot specify two servers "
                                             "with the same unique_id")
            self[serv.unique_id] = serv
