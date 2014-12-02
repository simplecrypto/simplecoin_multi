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
