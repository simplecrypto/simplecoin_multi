import requests
import collections
import simplejson as json
import bz2
import time

from flask import current_app
from cryptokit.rpc import CoinserverRPC
from cryptokit.base58 import address_version
from decimal import Decimal as dec
from urlparse import urljoin

from . import models as m
from . import (cache, redis_conn, currencies, chains, powerpools, locations,
               algos)
from .utils import time_format
from .exceptions import ConfigurationException, RemoteException, InvalidAddressException


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
        return str(self.key)

    def __repr__(self):
        return str("{} {}".format(self.__class__.__name__, self.key))

    def __hash__(self):
        return hash(self.key)


class ConfigChecker(ConfigObject):
    """
    This class provides various methods for validating config values and checks
    configuration values and makes sure they're properly filled out.

    It needs a lot of expansion :/

    Currently validates the following keys:
    pool_payout_addr
    currencies
    """

    def __init__(self, cfg, app):
        # A shim for now. This should be removed soon, as we move all global
        # config access to this object
        app.config.update(cfg)
        # Set main configuration object
        app.config_obj = self
        # Update values
        self.__dict__.update(cfg)

        # Check the 'currencies' key
        currencies = self.lookup_key('currencies')
        self.check_type(currencies, collections.Mapping)

        # Objectize child config objects
        # =======================================================================
        app.locations = LocationKeeper(cfg.pop('locations'))
        app.currencies = CurrencyKeeper(cfg.pop('currencies'))
        app.powerpools = PowerPoolKeeper(cfg.pop('mining_servers'))
        app.algos = AlgoKeeper(cfg.pop('algos'))
        app.chains = ChainKeeper(cfg.pop('chains'))

        # Verify global pool payout information
        pool_curr = cfg['pool_payout_currency']
        if pool_curr not in app.currencies:
            raise ConfigurationException("Invalid pool payout currency!")
        if app.currencies[pool_curr].exchangeable is not True:
            raise ConfigurationException("Pool payout currency must be exchangeable!")
        if app.currencies[pool_curr].pool_payout_addr is None:
            raise ConfigurationException("Pool payout currency must define a pool_payout_addr!")
        self.pool_payout_currency = app.currencies[pool_curr]

    def lookup_key(self, key, nested=None):
        """ Helper method: Checks the config for the specified key and raises
        an error if not found """

        try:
            if nested is None:
                value = self[key]
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
        if not isinstance(val, obj_type):
            raise ConfigurationException("\'{}\' is not an instance of {}"
                                         .format(val, obj_type.__name__))

    def check_is_bcaddress(self, val):
        """ Helper method: Checks a value for truthiness """
        try:
            ver = address_version(val)
        except (KeyError, AttributeError):
            raise ConfigurationException("\'{}\' is not a valid bitcoin style "
                                         "address".format(val))
        return ver


class Currency(ConfigObject):
    requires = ['algo', 'name', 'coinserv', 'address_version',
                'trans_confirmations', 'block_time', 'block_mature_confirms']
    defaults = dict(exchangeable=False,
                    minimum_payout='0.00000001',
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
        self.exchangeable = bool(self.exchangeable)
        self.minimum_payout = dec(self.minimum_payout)

        # If a pool payout addr is specified, make sure it matches the
        # configured address version.
        if self.pool_payout_addr is not None:
            try:
                ver = address_version(self.pool_payout_addr)
            except (KeyError, AttributeError):
                ver = None
            if ver not in self.address_version:
                raise ConfigurationException(
                    "{} is not a valid {} address. Must be version {}, got version {}"
                    .format(self.pool_payout_addr, self.key, self.address_version, ver))

        # Check to make sure there is a configured pool address for
        # unexchangeable currencies
        if self.exchangeable is False and self.pool_payout_addr is None:
            raise ConfigurationException(
                "Unexchangeable currencies require a pool payout addr."
                "No valid address found for {}".format(self.key))

    def __repr__(self):
        return self.key
    __str__ = __repr__

    def __hash__(self):
        return self.key.__hash__()


class CurrencyKeeper(dict):
    __getattr__ = dict.__getitem__

    def __init__(self, currency_dictionary):
        super(CurrencyKeeper, self).__init__()
        # If no mining currency is specified explicitly for username, then
        # we will use this map to lookup
        self.version_map = {}
        for key, config in currency_dictionary.iteritems():
            config['key'] = key
            obj = Currency(config)
            if key in self:
                raise ConfigurationException("Duplicate currency keys {}"
                                             .format(key))

            if obj.exchangeable:
                for version in obj.address_version:
                    if version in self.version_map:
                        raise ConfigurationException(
                            "Cannot have overlappting exchangeable address_versions."
                            "Tried to add {} for {}, but already has {}"
                            .format(version, obj, self.version_map[version]))
                    self.version_map[version] = obj
            self[obj.key] = obj

    @property
    def exchangeable_currencies(self):
        return [c for c in self.itervalues() if c.exchangeable is True]

    @property
    def unexchangeable_currencies(self):
        return [c for c in self.itervalues() if c.exchangeable is False]

    @property
    def available_versions(self):
        versions = {}
        for v in self.itervalues():
            for version in v.address_version:
                versions.setdefault(version, [])
                versions[version].append(v)
        return versions

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
        ver = self.validate_bc_address(address)

        try:
            return self.version_map[ver]
        except KeyError:
            raise InvalidAddressException(
                "Address '{}' version {} is not an exchangeable currency. Options are {}"
                .format(address, ver, self.exchangeable_currencies))

    def validate_bc_address(self, bc_address_str):
        """
        The go-to function for all your bitcoin style address validation needs.

        Expects to receive a string believed to represent a bitcoin address
        Raises appropriate errors if any checks are failed, otherwise returns a
        list of Currency objects that have the same addr version.
        """
        # First check to make sure the address contains only alphanumeric chars
        if not bc_address_str.isalnum():
            raise InvalidAddressException('Address should be alphanumeric')

        # Check to make sure str is the proper length
        if not len(bc_address_str) >= 33 or not len(bc_address_str) <= 35:
            raise InvalidAddressException('Address should be 33-35 characters long')

        # Check to see if the address can be looked up from the config
        try:
            ver = address_version(bc_address_str)
        except (AttributeError, ValueError):
            raise InvalidAddressException("Invalid")
        return ver


class Chain(ConfigObject):
    requires = ['type', 'valid_address_versions', 'fee_perc', '_algo']
    defaults = dict(block_bonus="0")
    max_indexes = 1000
    min_index = 0

    def __init__(self, bootstrap):
        bootstrap['_algo'] = bootstrap.pop('algo')
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

    @property
    def algo(self):
        return algos[self._algo]

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
        decoding_time = 0.0
        retrieval_time = 0.0
        aggregation_time = 0.0
        for index in xrange(start_slice, stop_slice, -1):
            slc_key = "chain_{}_slice_{}".format(self.id, index)
            key_type = redis_conn.type(slc_key)

            # Fetch slice information
            t = time.time()
            if key_type == "list":
                slc = dict(encoding="colon_list", data=redis_conn.lrange(slc_key, 0, -1))
            elif key_type == "hash":
                slc = redis_conn.hgetall(slc_key)
            elif key_type == "none":
                continue
            else:
                raise Exception("Unexpected slice key type {}".format(key_type))
            retrieval_time += time.time() - t

            # Decode slice information
            t = time.time()
            if slc['encoding'] == "bz2json":
                serialized = bz2.decompress(slc['data'])
                entries = json.loads(serialized, use_decimal=True)
            elif slc['encoding'] == "colon_list":
                # Parse the list into proper python representation
                entries = []
                for entry in slc['data']:
                    user, shares = entry.split(":")
                    shares = dec(shares)
                    entries.append((user, shares))
            else:
                raise Exception("Unsupported slice data encoding {}"
                                .format(slc['encoding']))
            decoding_time += time.time() - t

            entry_count = 0
            t = time.time()
            for user, shares in entries:
                assert isinstance(shares, (dec, int))
                if user not in users:
                    users[user] = shares
                else:
                    users[user] += shares
                entry_count += 1
                found_shares += shares
                if target_shares and found_shares >= target_shares:
                    break
            aggregation_time += time.time() - t

            if target_shares and found_shares >= target_shares:
                break

        current_app.logger.info(
            "Aggregated {:,} shares from {:,} entries for {:,} different users "
            "from slice #{:,} -> #{:,}. retrieval_time: {}; decoding_time: {}"
            " aggregation_time: {}"
            .format(found_shares, entry_count, len(users), start_slice, index,
                    time_format(retrieval_time), time_format(decoding_time),
                    time_format(aggregation_time)))

        return users


class PPLNSChain(Chain):
    requires = Chain.requires[:]
    requires.extend(['last_n'])

    def __init__(self, bootstrap):
        Chain.__init__(self, bootstrap)
        self.last_n = float(self.last_n)

    def calc_shares(self, block_payout):
        assert block_payout.chainid == self.id
        n = (block_payout.block.difficulty * (2 ** 32)) / self.algo.hashes_per_share
        target_shares = int(round(n * self.last_n))
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
            last_block_payout = [bp for bp in last_block.chain_payouts if bp.chainid == self.id][0]
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
            pass_cfg['key'] = id
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
            raise RemoteException("Non 200 from endpoint {}: {}"
                                  .format(url, ret.text.encode('utf8')[:100]))

        current_app.logger.debug("Got {} from remote"
                                 .format(ret.text.encode('utf8')))
        try:
            return ret.json()
        except ValueError:
            raise RemoteException("Non json from endpoint {}: {}"
                                  .format(url, ret.text.encode('utf8')[:100]))

    def full_info(self):
        return ("<id {}; chain {}; location {}; monitor_address {}>"
                .format(self.unique_id, self.chain, self.location,
                        self.monitor_address))

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
