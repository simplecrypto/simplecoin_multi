.. PowerPool documentation master file, created by
   sphinx-quickstart on Sat Nov  8 14:30:26 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SimpleCoin: A mining pool frontend
=====================================

SimpleCoin is an open source mining pool frontend, it performs many of the same
functions as software like MPOS. It is currently under active development for
use in the `SimpleMulti <http://simplemulti.com>`_ mining pool, although we are
gradually adding documentation so it can be set up more easily by others as
well.

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
* Translation support. Thanks to `sbwdlihao's <https://github.com/sbwdlihao>`_
  contributions we support internationalization.

Table of Contents
------------------

.. toctree::
   :maxdepth: 2

   setup.rst
   production.rst
   api.rst
