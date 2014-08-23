import subprocess
import logging
import os
import yaml
import sys
import setproctitle
import inspect

from apscheduler.threadpool import ThreadPool
from apscheduler.scheduler import Scheduler
from decimal import Decimal
from redis import Redis
from flask import Flask, current_app
from flask.ext.cache import Cache
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.migrate import Migrate
from jinja2 import FileSystemLoader
from werkzeug.local import LocalProxy
from autoex.ex_manager import ExchangeManager

import simplecoin.filters as filters


root = os.path.abspath(os.path.dirname(__file__) + '/../')
db = SQLAlchemy()
cache = Cache()
currencies = LocalProxy(
    lambda: getattr(current_app, 'currencies', None))
powerpools = LocalProxy(
    lambda: getattr(current_app, 'powerpools', None))
redis_conn = LocalProxy(
    lambda: getattr(current_app, 'redis', None))
exchanges = LocalProxy(
    lambda: getattr(current_app, 'exchanges', None))


def create_app(mode, config='/config.yml', log_level=None):
    # initialize our flask application
    app = Flask(__name__, static_folder='../static', static_url_path='/static')

    # set our template path and configs
    app.jinja_loader = FileSystemLoader(os.path.join(root, 'templates'))
    config_vars = dict(manage_log_file="manage.log",
                       webserver_log_file="webserver.log",
                       scheduler_log_file=None,
                       log_level='INFO',
                       worker_hashrate_fold=86400)
    config_vars.update(yaml.load(open(root + config)))
    # inject all the yaml configs
    app.config.update(config_vars)
    app.currencies = CurrencyKeeper(app.config['currencies'])
    app.powerpools = PowerPoolKeeper(app.config['mining_servers'])
    app.ports = PortKeeper(app.config['mining_servers'])
    app.exchanges = ExchangeManager(app.config['exchange_manager'])

    del app.logger.handlers[0]
    app.logger.setLevel(logging.NOTSET)
    log_format = logging.Formatter('%(asctime)s [%(name)s] [%(levelname)s]: %(message)s')
    log_level = getattr(logging, str(log_level), app.config['log_level'])

    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(log_format)
    logger.addHandler(handler)

    # Handle optionally adding log file writers for each different run mode
    if mode == "manage" and app.config['manage_log_file']:
        hdlr = logging.FileHandler(app.config['manage_log_file'])
        hdlr.setFormatter(log_format)
        logger.addHandler(hdlr)
    if mode == "scheduler" and app.config['scheduler_log_file']:
        hdlr = logging.FileHandler(app.config['scheduler_log_file'])
        hdlr.setFormatter(log_format)
        logger.addHandler(hdlr)
    if mode == "webserver" and app.config['webserver_log_file']:
        hdlr = logging.FileHandler(app.config['webserver_log_file'])
        hdlr.setFormatter(log_format)
        logger.addHandler(hdlr)

    logging.getLogger("gunicorn.access").setLevel(logging.WARN)
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.INFO)

    # add the debug toolbar if we're in debug mode...
    # ##################
    if app.config['DEBUG'] and mode == "webserver":
        # Log all stdout and stderr when in debug mode for convenience
        class LoggerWriter:
            def __init__(self, logger, level):
                self.logger = logger
                self.level = level

            def write(self, message):
                if message != '\n':
                    self.logger.log(self.level, message)

        sys.stdout = LoggerWriter(app.logger, logging.DEBUG)
        sys.stderr = LoggerWriter(app.logger, logging.DEBUG)

    # register all our plugins
    # ##################
    db.init_app(app)
    # Redis connection configuration
    cache_config = {'CACHE_TYPE': 'redis'}
    cache_config.update(app.config.get('main_cache', {}))
    cache.init_app(app, config=cache_config)
    app.redis = Redis(**app.config.get('redis_conn', {}))
    app.SATOSHI = Decimal('0.00000001')

    if mode == "manage":
        # Initialize the migration settings
        Migrate(app, db)
    elif mode == "webserver":
        # try and fetch the git version information
        try:
            output = subprocess.check_output("git show -s --format='%ci %h'",
                                             shell=True).strip().rsplit(" ", 1)
            app.config['hash'] = output[1]
            app.config['revdate'] = output[0]
        # celery won't work with this, so set some default
        except Exception:
            app.config['hash'] = ''
            app.config['revdate'] = ''

        # Dynamically add all the filters in the filters.py file
        for name, func in inspect.getmembers(filters, inspect.isfunction):
            app.jinja_env.filters[name] = func

        app.logger.info("Starting up SimpleCoin!\n{}".format("=" * 100))
    elif mode == "scheduler":
        current_app.logger.info("=" * 80)
        current_app.logger.info("SimpleCoin cron scheduler starting up...")
        setproctitle.setproctitle("simplecoin_scheduler")

        ThreadPool.app = app
        sched = Scheduler(standalone=True)
        # All these tasks actually change the database, and shouldn't
        # be run by the staging server
        if not app.config.get('stage', False):
            # every minute at 55 seconds after the minute
            sched.add_cron_job(sch.run_payouts, second=55)
            # every minute at 55 seconds after the minute
            sched.add_cron_job(sch.create_trade_req, args=("sell",), second=0)
            # every minute at 55 seconds after the minute
            sched.add_cron_job(sch.create_trade_req, args=("buy",), second=5)
            # every minute at 55 seconds after the minute
            sched.add_cron_job(sch.collect_minutes, second=35)
            # every five minutes 20 seconds after the minute
            sched.add_cron_job(sch.compress_minute, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=20)
            # every hour 2.5 minutes after the hour
            sched.add_cron_job(sch.compress_five_minute, minute=2, second=30)
            # every 15 minutes 2 seconds after the minute
            sched.add_cron_job(sch.update_block_state, second=2)
        else:
            current_app.logger.info("Stage mode has been set in the configuration, not "
                                    "running scheduled database altering cron tasks")

        sched.add_cron_job(sch.update_online_workers, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=30)
        sched.add_cron_job(sch.cache_user_donation, minute='0,15,30,45', second=15)
        sched.add_cron_job(sch.server_status, second=15)
        sched.start()

    # Route registration
    # =========================================================================
    from . import views, models, api, rpc_views
    app.register_blueprint(views.main)
    app.register_blueprint(rpc_views.main)
    app.register_blueprint(api.api, url_prefix='/api')

    return app


def create_manage_app(**kwargs):
    app = create_app("manage", **kwargs)

    return app


from .utils import CurrencyKeeper, PowerPoolKeeper, PortKeeper
import simplecoin.scheduler as sch
