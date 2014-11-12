SimpleCoin Multi
================
[![Build Status](https://travis-ci.org/simplecrypto/simplecoin_multi.svg?branch=master)](https://travis-ci.org/simplecrypto/simplecoin_multi)
[![Coverage Status](https://coveralls.io/repos/simplecrypto/simplecoin_multi/badge.png?branch=master)](https://coveralls.io/r/simplecrypto/simplecoin_multi?branch=master)
[![Documentation Status](https://readthedocs.org/projects/simplecoin-multi/badge/?version=latest)](https://readthedocs.org/projects/simplecoin-multi/?badge=latest)

SimpleCoin is an open source mining pool frontend, similar to MPOS in some
ways. It is currently under active development for use in the
[SimpleMulti](http://simplemulti.com) mining pool, although we are gradually
adding documentation so others can use it as well.

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

Production Setup
================
Currently we have no documentation on how to properly setup SimpleCoin for
production. You're welcome to ask in our IRC #simplecrypto but we won't always
have time to help. By reading through the development instructions below you
might be able to set it up, but we take no responsibilities for security
problems that arise if you try to use it in production.

Development
================

Setting up the dev enviroment
-----------------------------
To start a local webserver running your code, follow these steps:

``` bash
# If you don't already have virtualenvwrapper, I recommend you get it
sudo apt-get install virtualenvwrapper  # on Ubuntu
sudo pip install virtualenvwrapper  # on most other systems
# Create a new virtual enviroment to isolate our code
mkvirtualenv scm
# Install all our dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .
# Run an instance of the scheduler and a webserver. The scheduler runs periodic
# tasks like processing shares from the mining server
env SIMPLECOIN_CONFIG=example.toml supervisord -c supervisor.conf
```

Mining
-----------------------------
If you want to start sending shares and solved blocks to Redis for this dev
instance to process, head over to
[powerpool](https://github.com/simplecrypto/powerpool) and read it's setup
instructions. You'll likely want to run a local [testnet in a
box](https://github.com/freewil/bitcoin-testnet-box), or on the live testnet.
We recommend the litecoin testnet for testing.

Payouts
-----------------------------
If you want to run a test payout use the
[simplecoin_rpc_client](http://github.com/simplecrypto/simplecoin_rpc_client)

Autoexchanging
-----------------------------
This project does not include all of the code required to perform automatic
exchanging. End-to-end functionality is possible if you manually exchange the
funds (or write your own code). An autoexchanging service may be offered in the 
future, but the code will remain our secret sauce.

Running the tests
-----------------------------
Before sending pull requests for changes, it's a good idea to run the tests.

``` bash
# enter our virtualenv
workon scm
# the tests require some special dependencies
pip install -r requirements-test.txt
nosetests
# If you want coverage
nosetests --with-coverage --cover-package=simplecoin --cover-html 
```

Building the docs
-----------------------------
``` bash
workon scm
# install sphinx
pip install -r requirements-docs.txt
cd doc/
make html
# view it in your browser
google-chrome _build/html/index.html
```
