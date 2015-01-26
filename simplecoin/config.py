import requests
import collections
import simplejson as json
import bz2
import time
import toml
import copy

from flask import current_app
from cryptokit.rpc import CoinserverRPC
from cryptokit.base58 import address_version
from decimal import Decimal as dec

from . import models as m
from . import (redis_conn, chains, powerpools, locations, algos, global_config,
               currencies)
from .utils import time_format
from .exceptions import (ConfigurationException, RemoteException,
                         InvalidAddressException)


class ConfigObject(dict):
    __getattr__ = dict.__getitem__
    requires = []
    defaults = dict()

    def __init__(self, bootstrap):
        super(ConfigObject, self).__init__()
        # Default settings
        self.__dict__.update(self.defaults)
        self.__dict__.update(bootstrap)
        for req in self.requires:
            if not hasattr(self, req):
                raise ConfigurationException(
                    "{} item '{}' with key {} requires {}"
                    .format(self.__class__.__name__, self, self.key, req))

    def __str__(self):
        return str(self.key)

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.key)

    def __nonzero__(self):
        return True

    __bool__ = __nonzero__

    def __gt__(self, other):
        if type(other) != type(self):
            return False
        return self.key.__gt__(other.key)

    def __ge__(self, other):
        if type(other) != type(self):
            return False
        return self.key.__ge__(other.key)

    def __le__(self, other):
        if type(other) != type(self):
            return False
        return self.key.__le__(other.key)

    def __lt__(self, other):
        if type(other) != type(self):
            return False
        return self.key.__lt__(other.key)

    def __ne__(self, other):
        if type(other) != type(self):
            return False
        return self.key.__ne__(other.key)

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        return self.key.__eq__(other.key)

    def __hash__(self):
        return hash(self.key)


class Keeper(dict):
    def __init__(self, configs):
        super(Keeper, self).__init__()
        defaults = configs.pop('default', {})
        for key, cfg in configs.iteritems():
            cfg = toml.toml_merge_dict(copy.deepcopy(defaults), cfg)
            cfg['key'] = key
            typ = cfg.get('type', 'default')
            obj = self.type_map[typ](cfg)
            if key in self:
                # XXX: Make more descriptive
                raise ConfigurationException("Duplicate keys {}".format(key))
            self[obj.key] = obj


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
        self.update(cfg)

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
        if app.currencies[pool_curr].buyable is not True:
            raise ConfigurationException("Pool payout currency must be buyable!")
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
    requires = ['_algo', 'name', 'address_version', 'trans_confirmations',
                'block_time', 'block_mature_confirms']
    defaults = dict(sellable=False,
                    buyable=False,
                    merged=False,
                    minimum_payout='0.00000001',
                    coinserv={},
                    pool_payout_addr=None,
                    block_explore=None,
                    tx_explore=None
                    )

    def __init__(self, bootstrap):
        bootstrap['_algo'] = bootstrap.pop('algo', None)
        if bootstrap['_algo'] is None:
            raise ConfigurationException(
                "A currency in config.toml is missing an entry in "
                "defaults.toml! The following config may help identify it: {}"
                .format(bootstrap))

        ConfigObject.__init__(self, bootstrap)
        if self.coinserv:
            cfg = self.coinserv
            self.coinserv = CoinserverRPC(
                "http://{0}:{1}@{2}:{3}/"
                .format(cfg['username'],
                        cfg['password'],
                        cfg['address'],
                        cfg['port'],
                        pool_kwargs=dict(maxsize=bootstrap.get('maxsize', 10))))
            self.coinserv.config = cfg
        elif self.sellable or self.mineable or self.buyable:
            raise ConfigurationException(
                "Coinserver must be configured for {}!".format(self.key))

        self.sellable = bool(self.sellable)
        self.buyable = bool(self.buyable)
        self.merged = bool(self.merged)
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
        # unsellable currencies
        if self.sellable is False and self.pool_payout_addr is None and self.mineable:
            raise ConfigurationException(
                "Unsellable currencies require a pool payout addr."
                "No valid address found for {}".format(self.key))

    @property
    def algo(self):
        return algos[self._algo]

    @property
    def pool_payout(self):
        # Get the pools payout information for this block
        global_curr = global_config.pool_payout_currency
        pool_payout = dict(address=self.pool_payout_addr,
                           currency=self,
                           user=global_curr.pool_payout_addr)
        # If this currency has no payout address, switch to global default
        if pool_payout['address'] is None:
            pool_payout['address'] = global_curr.pool_payout_addr
            pool_payout['currency'] = global_curr
            # Double check
            assert self.sellable is True, "Block is un-sellable"

        # Double check valid. Paranoid
        address_version(pool_payout['address'])
        return pool_payout


class CurrencyKeeper(Keeper):
    type_map = dict(default=Currency)

    def __init__(self, configs):
        super(CurrencyKeeper, self).__init__(configs)

        # If no mining currency is specified explicitly for username, then
        # we will use this map to lookup. It needs to be unique so there's no
        # currency ambiguity
        self.version_map = {}
        for obj in self.values():
            # Ignore currency objects that aren't setup
            if not obj.sellable and not obj.mineable and not obj.buyable:
                self.pop(obj.key)

            # Don't add to the version map if not buyable
            if not obj.buyable:
                continue

            for version in obj.address_version:
                if version in self.version_map:
                    raise ConfigurationException(
                        "Cannot have overlappting buyable address_versions."
                        "Tried to add {} for {}, but already has {}"
                        .format(version, obj, self.version_map[version]))
                self.version_map[version] = obj

    @property
    def buyable_currencies(self):
        return [c for c in self.itervalues() if c.buyable is True]

    @property
    def unbuyable_currencies(self):
        return [c for c in self.itervalues() if c.buyable is False]

    @property
    def sellable_currencies(self):
        return [c for c in self.itervalues() if c.sellable is True]

    @property
    def unsellable_currencies(self):
        return [c for c in self.itervalues() if c.sellable is False]

    @property
    def unmineable_currencies(self):
        return [c for c in self.itervalues() if c.mineable is False]

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
        Checks an address to determine if its a valid and payable(buyable)
        address. Typically used to validate a username address.
        Returns the payable currency object for that version.

        When calling this function you should always expect exceptions to be
        raised.

        !!! This function assumes that a currency will not be configured as
        buyable if there is a version conflict with another currency.

        Although it makes this assumption - it should return a consistent
        Currency obj even if configuration is incorrect
        """
        ver = self.validate_bc_address(address)

        try:
            return self.version_map[ver]
        except KeyError:
            raise InvalidAddressException(
                "Address '{}' version {} is not an buyable currency. Options are {}"
                .format(address, ver, self.buyable_currencies))

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
    requires = ['type', 'fee_perc', '_algo', '_currencies', 'safety_margin']
    defaults = dict(block_bonus="0", currencies=[], safety_margin=2)
    max_indexes = 1000
    min_index = 0

    def __init__(self, bootstrap):
        bootstrap['_algo'] = bootstrap.pop('algo')
        bootstrap['_currencies'] = bootstrap.pop('currencies')
        bootstrap['key'] = int(bootstrap['key'])
        ConfigObject.__init__(self, bootstrap)
        self.id = self.key

        assert isinstance(self.fee_perc, basestring)
        assert isinstance(self.block_bonus, basestring)
        self.fee_perc = dec(self.fee_perc)
        self.hr_fee_perc = round(self.fee_perc * 100, 2)

    @property
    def currencies(self):
        return [currencies[curr] for curr in self._currencies]

    @property
    def algo(self):
        return algos[self._algo]

    def calc_shares(self, block_payout):
        """ Pass a block_payout object with only chain ID and blockhash
        populated and compute share amounts """
        raise NotImplementedError

    def _calc_shares(self, start_slice, target_shares=None, stop_slice=None):
        if target_shares is not None and target_shares <= 0:
            raise ValueError("Taget shares ({}) must be positive"
                             .format(target_shares))
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
        entry_count = 0
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
        target_shares = n * self.last_n
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
        stop_slice = 0
        if last_block:
            bps = [bp for bp in last_block.chain_payouts if bp.chainid == self.id]
            if len(bps) > 0:
                last_block_payout = bps[0]
                stop_slice = last_block_payout.solve_slice
        return self._calc_shares(block_payout.solve_slice, stop_slice=stop_slice)


class ChainKeeper(Keeper):
    type_map = {"pplns": PPLNSChain, "prop": PropChain}


class Location(ConfigObject):
    required = ['location_acronym', 'location', 'country_flag', 'address']

    def stratums_by_algo(self):
        by_algo = {}
        for strat in powerpools.itervalues():
            if strat._location == self.key:
                lst = by_algo.setdefault(strat.chain.algo, [])
                lst.append(strat)
        return by_algo


class LocationKeeper(Keeper):
    type_map = dict(default=Location)


class Algo(ConfigObject):
    defaults = dict(enabled=True)


class AlgoKeeper(Keeper):
    type_map = dict(default=Algo)

    def active_algos(self):
        return [a for a in self.itervalues() if a.enabled]


class PowerPool(ConfigObject):
    timeout = 10
    requires = ['_chain', 'port', 'address', 'monitor_address', '_location']

    def __init__(self, bootstrap):
        bootstrap['_chain'] = bootstrap.pop('chain')
        bootstrap['_location'] = bootstrap.pop('location')
        bootstrap['key'] = int(bootstrap['key'])
        ConfigObject.__init__(self, bootstrap)
        self.id = self.key

    @property
    def stratum_address(self):
        return "stratum+tcp://{}:{}".format(self.address, self.port)
    display_text = stratum_address

    __repr__ = lambda self: self.monitor_address
    __str__ = lambda self: self.monitor_address

    def __hash__(self):
        return self.key

    def request(self, url, method='GET', max_age=None, signed=True, **kwargs):
        url = "{}/{}".format(self.monitor_address.rstrip('/'), url.lstrip('/'))
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
                .format(self.key, self.chain, self.location,
                        self.monitor_address))

    @property
    def location(self):
        return locations[self._location]

    @property
    def chain(self):
        return chains[self._chain]


class PowerPoolKeeper(Keeper):
    type_map = dict(default=PowerPool)
