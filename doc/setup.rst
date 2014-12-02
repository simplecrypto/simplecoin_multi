Getting Setup
=============

SimpleCoinMulti uses PostgreSQL as its primary database, although SCM is
configurable and allows using pretty much any database supported via SQLAlchemy.
Setup is tested running on Ubuntu 12.04. If you're doing development you'll
also want to install Supervisor for convenience.

Installation
------------

To install thse packages on Ubuntu 12.04:

.. code-block:: bash

    apt-get install redis-server postgresql-contrib-9.3 postgresql-9.3 postgresql-server-dev-9.3
    # to install supervisor as well
    apt-get install supervisor

Now you'll want to setup a Python virtual enviroment to run the application in.
This isn't stricly necessary, but not using virtualenv can cause all kinds of
headache, so it's *highly* recommended. You'll want to setup virtualenvwrapper
to make this easier.

.. code-block:: bash

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

.. code-block:: bash

    # creates a new user with password 'testing', creates the database
    ./util/reset_db.sh
    # creates the database schema for simpledoge
    python manage.py init_db

Now everything should be ready for running the server. This project uses
supervisor in development to watch for file changes and reload the server.

.. code-block:: bash

    supervisord -c supervisor.conf

This should successfully start both the development server and the task
scheduler if all is well. If not, carefully reading the output from supervisor
should give good hints on what's not working right.

Mining
------

If you want to start sending shares and solved blocks to Redis for this dev
instance to process, head over to
`powerpool <https://github.com/simplecrypto/powerpool>`_ and read it's setup
instructions. You'll likely want to run a local `testnet in a
box <https://github.com/freewil/bitcoin-testnet-box>`_, or on the live testnet.
We recommend the litecoin testnet for testing.

Payouts & Manual Exchanging
---------------------------

The RPC client works with SimpleCoin Multi's RPC views. This can be run on a
secure server to pull payout and trade data. This client is what actually makes
the payouts, and the ``simplecoin_rpc_client`` allows manually managing
exchanging.

`simplecoin_rpc_client <http://github.com/simplecrypto/simplecoin_rpc_client>`_

Unfortunately docs for how to use this (especially in a production setting) are
very lacking at the moment.

Autoexchanging
-----------------------------

We currently offer no code to perform automatic exchanging, although you could
expand the RPC client to do it, or write your own app to handle it. A first
class autoexchanging service may be offered by us at some point in the future.
