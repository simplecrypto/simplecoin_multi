SimpleCoin Multi
================

[![Build Status](https://travis-ci.org/simplecrypto/simplecoin_multi.svg?branch=master)](https://travis-ci.org/simplecrypto/simplecoin_multi)
[![Coverage Status](https://coveralls.io/repos/simplecrypto/simplecoin_multi/badge.png?branch=master)](https://coveralls.io/r/simplecrypto/simplecoin_multi?branch=master)
[![Documentation Status](https://readthedocs.org/projects/simplecoin-multi/badge/?version=latest)](https://readthedocs.org/projects/simplecoin-multi/?badge=latest)

A multipool version of simplecoin.

Semi-Functional at the moment!
-----------------------------

**This project is quite green, and does not include some of the code required
to perform automatic exchanging. End-to-end functionality is possible if you
manually exchange the funds (or write your own code). Be prepared to dig into
the code a bit, as a lot of this is pretty green and often poorly documented**

**To get payouts going you'll probably want to look at http://github.com/simplecrypto/simplecoin_rpc_client**

Running the dev enviroment
-----------------------------
To start a local webserver running your code, follow these steps:

```` bash
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

Running the tests
-----------------------------
Before sending pull requests for changes, it's a good idea to run the tests.

```` bash
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

```` bash
workon scm
# install sphinx
pip install -r requirements-docs.txt
cd doc/
make html
# view it in your browser
google-chrome _build/html/index.html
```
