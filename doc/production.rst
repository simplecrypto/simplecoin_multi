Production Installation
==============================

Currently we have very limited documentation on how to properly get setup for
production. SimpleCoin Multi is still a pretty green package. By reading
through the "Getting Setup" section you should be able to get an idea of
how you might set it up in production, but we take no responsibilities for
problems that arise. If you have questions you're welcome to ask in
our IRC #simplecrypto but we won't always have time to help.

General Tips
-----------------------------

* It is a good idea to use a process control package like Supervisor or Upstart
  to run & manage gunicorn, nginx, the scheduler, and any other mission
  critical processes (ie, PowerPool, coin daemons, etc). This allows easier log
  management, user control, automatically restarts, etc.
* Optimal scheduled task intervals may be different than the default, so
  reading through the different tasks and understanding what they do and
  tweaking their schedules accordingly is a good idea.
* Gunicorn is the webserver that runs SimpleCoin, but it is lacking in many
  desirable features like caching and rate limiting. Because of this it's
  highly recommended that you put a HTTP reverse proxy server in front of it.
  NGINX is what we run in production, but Apache and others could be used as
  well.

Configuration
-------------

The configuration file has a lot of options, and at the moment they're not
documented the best. It's recommended that you read through the
``example.toml`` and ``defaults.toml`` to get handle on it. We hope to have
sections explaining sharechains and mining server configuration in detail soon,
but at the moment these topics are a bit deep and will likely be confusing
without reading quite a bit of code.

Scheduler
---------

The scheduler handles things like pulling PowerPool's share data out of redis,
generating various cached values, creating payouts & trade requests, and many
other vital tasks.  To get it running you'll first need to set an environment
variable with the path to the config file. It should look something like this:

.. code-block:: bash

    export SIMPLECOIN_CONFIG=/home/$USER/simplecoin_multi/config.toml
    python simplecoin/scheduler.py

A simple upstart script would look something like this:

.. code-block:: text

    start on (filesystem)
    stop on runlevel [016]
    respawn
    console log
    setuid multi
    setgid multi
    env SIMPLECOIN_CONFIG=/home/multi/web/config.toml

    exec /home/multi/web_venv/bin/simplecoin_scheduler

Webserver
---------

The webserver handler user requests. An example upstart configuration:

.. code-block:: text

    start on (filesystem)
    stop on runlevel [016]

    respawn
    console log
    setuid multi
    setgid multi
    chdir /home/multi/web
    env SIMPLECOIN_CONFIG=/home/multi/web/config.toml

    exec /home/multi/web_venv/bin/gunicorn simplecoin.wsgi_entry:app -b 127.0.0.1:8080 --timeout 270

Adding A Currency
=================

Currency Configuration
----------------------

SimpleCoin has configuration sections that specify certain coin specific
parameters. Several common coins are already defined in ``defaults.toml``. If
your currency is not defined in the defaults file then you will have to add it.
It's best practice to add this information to your configuration, and not ``defaults.toml``.

* *name* - the currency name as displayed throughout the site. This is purely cosmetic.
* *algo* - the name of a configured algo. These are also defined in the
  defaults.toml. If you're using a new algorithm then you'll have to add an
  algorithm entry to the configs as well. Contact us or the coin creator to
  determine these details.
* *address_version* - This is a number that is a valid prefix for an address on
  this coin network. See below for figuring this out.
* *merged* - is this a merge mined coin?
* *block_mature_confirms* - the number of blocks required to have passed before
  you can spend a coinbase transaction. Usually defined by COINBASE_MATURITY in
  the main.h of the core client.
* *block_time* - the target block time in seconds
* *block_explore* - a url prefix to which a block hash can be looked up
* *tx_explore* - url prefix to which a transaction hash can be looked up

After you've added this information you'll now also need to define coinserver
information and exchangeability. If you're not exchanging coins than nothing is
buyable or sellable except the "pool payout" currency. This can be seen in the
``example.toml`` and is quite straightforward.

Chain Configuration
----------------------

Chains are a little complicated to explain succinctly. They are essentially a
way by which profit switching mining servers and dedicated currency mining
servers could work together to mine blocks, even with different payout methods
defined on each (PPLNS or PROP, etc). This is quite powerful, but also a bit complex.

If you're simply doing dedicated currency mining servers with no profit
switching or merged mining of any kind then each currency will be on its own
chain. So to setup your new currency you must add a new chain to the
configuration file. Make sure the chain number is unique, and never overlap
them!

* *title* - a description to display on the web interface
* *currencies* - a list of currencies that can be mined on the chain. So just
  the currency code you defined above.
* *algo* - the hashing algorithm of all currencies on this chain
* *type* - the method used to determine payouts for shares on the chain. Usually "pplns" is best as "prop" is poorly tested.
* *last_n* - pplns configuration
* *fee_perc* - a percentage to extract from earnings on this chain. given as a
  string such as "0.01" for 1%

Mining Server
-------------

This block should tie to a mining port on a powerpool instance. Keep in mind
that a single powerpool instance may contain multiple stratum ports, and each
stratum port should have it's own "mining_servers" configuration block in the
configuration. 

* *address* - A stratum address that users can point at, minus the port
* *monitor_address* - a url for the internal JSON monitor of this stratum port.
  Don't confuse this with powerpool's monitor URL. Each component within
  powerpool has it's own sub-address, and each mining port in powerpool is a
  component, so this should look something like ``http://localhost:[monitor
  port]/[name of stratum component``.
* *port* - the stratum addresses port
* *location* - the location configuration information. This needs to correspond to a location configuration block. Sorry, this seemed cool when we added it....
* *diff* - a text representation of the difficulty for this port. If it's vardiff, represent as a range.
* *chain* - the most important bit, which mining chain will this be on
