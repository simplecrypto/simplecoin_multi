Simple Doge
===========

This repo is a generic version of http://simpledoge.com and http://simplevert.com.
This includes all Celery tasks for handling the PowerPool stratum mining servers
output.

Getting Started
===============

Simple Doge makes use of PostgreSQL and Redis, as well as RabbitMQ if you'll
be running a test powerpool instance for end to end testing. Setup is designed
to run on Ubuntu 12.04. If you're doing development you'll also want to install
Node since Grunt is used.

    apt-get install redis-server postgresql-contrib-9.1 postgresql-9.1 postgresql-server-dev-9.1 
    # to install rabbitmq as well
    apt-get install rabbitmq-server
    # add the ppa that includes latest version of nodejs. Ubuntu repos are really out of date
    sudo add-apt-repository ppa:chris-lea/node.js
    sudo apt-get install nodejs

Now you'll want to setup a Python virtual enviroment to run the application in.
This isn't stricly necessary, but not using virtualenv can cause all kinds of 
headache, so it's *highly* recommended. You'll want to setup virtualenvwrapper 
to make this easier.

    # make a new virtual enviroment for simpledoge
    mkvirtualenv sd
    # clone the source code repo
    git clone https://github.com/ericecook/simpledoge.git
    cd simpledoge
    pip install -e .
    # install all python dependencies
    pip install -r requirements.txt
    pip install -r dev-requirements.txt
    # install nodejs dependencies for grunt
    sudo npm install -g grunt-cli  # setup grunt binary globally
    npm install  # setup all the grunt libs local to the project

Initialize an empty PostgreSQL database for simpledoge.

    # creates a new user with password testing, creates the database, enabled
    # contrib extensions
    ./util/reset_db.sh
    # creates the database schema for simpledoge
    python manage.py init_db

Now everything should be ready for running the server. This project uses Grunt
in development to watch for file changes and reload the server.

    grunt watch

This should successfully start the development server if all is well. If not,
taking a look at the uwsgi log can help.

    tail -f websever.log
    
It's also Usually I have this running in a
possible that gunicorn is failing to start completely, in which case you can run it
by hand to see what's going wrong.
    
    gunicorn simplecoin.wsgi_entry:app -D -p gunicorn.pid -b 0.0.0.0:9400 --access-logfile gunicorn.log
    
If you're running powerpool as well you'll need to start a celery worker to process
the tasks (found shares/blocks/stats etc) that it generates. You can run the worker
like this:
    
    python simplecoin/celery_entry.py -l INFO --beat
