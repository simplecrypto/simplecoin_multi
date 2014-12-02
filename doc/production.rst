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

