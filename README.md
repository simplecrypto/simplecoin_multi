SimpleCoin Multi
================
[![Build Status](https://travis-ci.org/simplecrypto/simplecoin_multi.svg?branch=master)](https://travis-ci.org/simplecrypto/simplecoin_multi)
[![Coverage Status](https://coveralls.io/repos/simplecrypto/simplecoin_multi/badge.png?branch=master)](https://coveralls.io/r/simplecrypto/simplecoin_multi?branch=master)
[![Documentation Status](https://readthedocs.org/projects/simplecoin-multi/badge/?version=latest)](https://readthedocs.org/projects/simplecoin-multi/?badge=latest)

SimpleCoin is an open source mining pool frontend, it performs many of the same
functions as software like MPOS. It is currently under active development for
use in the [SimpleMulti](http://simplemulti.com) mining pool, although we are
gradually adding documentation so it can be set up more easily by others as well.

Features
-----------------------------
* Multi-currency support. One instance of this pool software supports mining
  and paying out many currencies.
* Multi-algo support. Currently configured for X11, Scrypt, and Scrypt-N, but
  designed so others could be added.
* Merge mining enabled. We support merge mining as many currencies as you'd
  like.
* Multi-chain support. Some miners can mine with PPLNS, while others use PROP
  payout, but they work together to solve blocks faster for each "chain".
* Translation support. Thanks to [sbwdlihao's](https://github.com/sbwdlihao)
  contributions we support internationalization.


Developement Setup
==============================

SimpleCoin Multi Installation
-----------------------------
SimpleCoinMulti uses PostgreSQL as its primary database, although SCM is
configurable and allows using pretty much any database supported via SQLAlchemy.
Setup is tested running on Ubuntu 12.04. If you're doing development you'll
also want to install Supervisor for convenience.

    apt-get install redis-server postgresql-contrib-9.3 postgresql-9.3 postgresql-server-dev-9.3
    # to install supervisor as well
    apt-get install supervisor

Now you'll want to setup a Python virtual enviroment to run the application in.
This isn't stricly necessary, but not using virtualenv can cause all kinds of
headache, so it's *highly* recommended. You'll want to setup virtualenvwrapper
to make this easier.

    # make a new virtual environment for simplecoin multi
    mkvirtualenv scm
    # clone the source code repo
    git clone git@github.com:simplecrypto/simplecoin_multi.git
    cd simplecoin_multi
    pip install -e .
    # install all python dependencies
    pip install -r requirements.txt
    pip install -r dev-requirements.txt

Initialize an empty database & add tables

    # creates a new user with password 'testing', creates the database
    ./util/reset_db.sh
    # creates the database schema for simpledoge
    python manage.py init_db

Now everything should be ready for running the server. This project uses
supervisor in development to watch for file changes and reload the server.

    supervisord -c supervisor.conf

This should successfully start the development server if all is well. If not,
taking a look at the supervisor log can help.

    tail -f supervisord.log

It's also possible that gunicorn is failing to start completely, in which case you can run it
by hand to see what's going wrong.

    gunicorn simplecoin.wsgi_entry:app -R -b 0.0.0.0:9400

To perform various periodic tasks, you'll need to start the scheduler. It does
things like pulling PowerPool's share data out of redis, generating various
cached values, creating payouts & trade requests, and many other vital tasks.
To get it running you'll first need to set an environment variable with the
path to the config file. It should look something like this:

    export SIMPLECOIN_CONFIG=/home/$USER/simplecoin_multi/config.toml
    python simplecoin/scheduler.py

If you already have PowerPool setup you should now be good to start testing
things out!

Next Steps - Mining
-----------------------------
If you want to start sending shares and solved blocks to Redis for this dev
instance to process, head over to
[powerpool](https://github.com/simplecrypto/powerpool) and read it's setup
instructions. You'll likely want to run a local [testnet in a
box](https://github.com/freewil/bitcoin-testnet-box), or on the live testnet.
We recommend the litecoin testnet for testing.

Next Steps - Payouts & Manual Exchanging
----------------------------------------
The RPC client works with SCM's RPC views. This can be run on a secure server
to pull payout and trade data. This client is what actually makes the payouts,
and the simplecoin_rpc_client allows manually managing exchanging.
[simplecoin_rpc_client](http://github.com/simplecrypto/simplecoin_rpc_client)

Next Steps - Autoexchanging
-----------------------------
We currently offer no code to perform automatic exchanging, although you could
expand the RPC client to do it, or write your own app to handle it. A first
class autoexchanging service may be offered by us at some point in the future.


Production Installation
==============================
Currently we have very limited documentation on how to properly setup SCM for
production. SimpleCoin Multi is still a pretty green package. By reading
through the development instructions above you should be able to get an idea of
how you might set it up in production, but we take no responsibilities for
problems that arise. If you have questions you're welcome to ask in
our IRC #simplecrypto but we won't always have time to help.

Webserver Installation
-----------------------------
The development environment uses Gunicorn as its primary webserver, and this
is still do-able for production, but Gunicorn develops problems quickly under
load. Gunicorn recommends you put a HTTP proxy server in front of it (such as
nginx).

Process management
-----------------------------
It is a good idea to use a process control package like Supervisor or Upstart
to run & manage gunicorn, nginx, the scheduler, and any other mission critical
processes (ie, PowerPool, coin daemons, etc). This allows easier log
management, user control, automatically restarts, etc.


Building the docs
==============================
``` bash
workon scm
# install sphinx
pip install -r requirements-docs.txt
cd doc/
make html
# view it in your browser
google-chrome _build/html/index.html
```

Contribute
===============

Code
---------------
We're actively working on SCM, so please feel free to send in pull requests.
Before sending a pull request, it's a good idea to run the tests:

``` bash
# enter our virtualenv
workon scm
# the tests require some special dependencies
pip install -r requirements-test.txt
nosetests
# If you want coverage
nosetests --with-coverage --cover-package=simplecoin --cover-html
```

For pull requests that touch on especially sensitive areas, such as payout
calculations, share processing, etc, we will want to thoroughly test the changes
before merging it in. This may take us a while, so including tests for your code
and keeping it clean help speed things up.

Donate
---------------
If you feel so inclined, you can give back to the devs:

DOGE DAbhwsnEq5TjtBP5j76TinhUqqLTktDAnD

BTC 185cYTmEaTtKmBZc8aSGCr9v2VCDLqQHgR

VTC VkbHY8ua2TjxdL7gY2uMfCz3TxMzMPgmRR


Credits
---------------

Whether in debugging & troubleshooting, code contributions, or donations it is
very much appreciated, and critical to keep this project going. A big thank you
to those who have contributed:

* [sigwo](https://github.com/sigwo)
* [sbwdlihao](https://github.com/sbwdlihao)