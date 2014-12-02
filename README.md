SimpleCoin Multi
================
[![Build Status](https://travis-ci.org/simplecrypto/simplecoin_multi.svg?branch=master)](https://travis-ci.org/simplecrypto/simplecoin_multi)
[![Coverage Status](https://coveralls.io/repos/simplecrypto/simplecoin_multi/badge.png?branch=master)](https://coveralls.io/r/simplecrypto/simplecoin_multi?branch=master)
[![Documentation Status](https://readthedocs.org/projects/simplecoin-multi/badge/?version=latest)](https://readthedocs.org/projects/simplecoin-multi/?badge=latest)

SimpleCoin is an open source mining pool frontend, it performs many of the same
functions as software like MPOS. It is currently under active development for
use in the [SimpleMulti](http://simplemulti.com) mining pool, although we are
gradually adding documentation so it can be set up more easily by others as
well. It should be considered beta software, and breaking/sweeping changes may
still be added occasionally. We are following Semantic Versioning, and as such
once we reach v1.0 breaking changes will be less frequent.

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

For installation instructions please [read the
docs](http://simplecoin-multi.readthedocs.org/en/latest/).

Questions
=========
If you're having trouble you can ask on the mailing list.

#### [Simplecrypto Development Google Group](https://groups.google.com/forum/#!forum/simplecrypto-dev)

Contribute
===============

Code
---------------
We're actively working on SCM, so please feel free to send in pull requests.
Before sending a pull request, it's a good idea to run the tests. Code that
breaks tests will not be merged until it's fixed.

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
and keeping it clean will help speed things up.

Documentation
------------------------------
Documentation changes are very welcome. Before committing and making your pull
request you can build you changes locally if needed with the following:

``` bash
workon scm
# install sphinx
pip install -r requirements-docs.txt
cd doc/
make html
# view it in your browser
google-chrome _build/html/index.html
```

Chedda
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
