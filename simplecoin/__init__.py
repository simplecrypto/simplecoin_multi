import subprocess
import logging
import os
import yaml
import socket
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

import simplecoin.filters as filters


socket.setdefaulttimeout(10)


root = os.path.abspath(os.path.dirname(__file__) + '/../')
db = SQLAlchemy()
cache = Cache()
# ConfigKeeper proxies
currencies = LocalProxy(
    lambda: getattr(current_app, 'currencies', None))
global_config = LocalProxy(
    lambda: getattr(current_app, 'config_obj', None))
locations = LocalProxy(
    lambda: getattr(current_app, 'locations', None))
powerpools = LocalProxy(
    lambda: getattr(current_app, 'powerpools', None))
algos = LocalProxy(
    lambda: getattr(current_app, 'algos', None))
chains = LocalProxy(
    lambda: getattr(current_app, 'chains', None))

redis_conn = LocalProxy(
    lambda: getattr(current_app, 'redis', None))
exchanges = LocalProxy(
    lambda: getattr(current_app, 'exchanges', None))


def create_app(mode, config='config.yml', log_level=None, **kwargs):

    # Initialize our flask application
    # =======================================================================
    app = Flask(__name__, static_folder='../static', static_url_path='/static')

    # Set our template path and configs
    # =======================================================================
    app.jinja_loader = FileSystemLoader(os.path.join(root, 'templates'))
    config_vars = dict(manage_log_file="manage.log",
                       webserver_log_file="webserver.log",
                       scheduler_log_file=None,
                       log_level='INFO',
                       worker_hashrate_fold=86400)
    if os.path.isabs(config):
        config_path = config
    else:
        config_path = os.path.join(root, config)
    config_vars.update(yaml.load(open(config_path)))
    config_vars.update(**kwargs)

    # Objectizes all configurations
    # =======================================================================
    ConfigChecker(config_vars, app)

    # Setup logging
    # =======================================================================
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
    # =======================================================================
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

    # Add the debug toolbar if we're in debug mode
    # =======================================================================
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

    # Register the DB + Cache
    # =======================================================================
    db.init_app(app)
    # Redis connection configuration
    cache_config = {'CACHE_TYPE': 'redis'}
    cache_config.update(app.config.get('main_cache', {}))
    cache.init_app(app, config=cache_config)
    # Redis connection for persisting application information
    app.redis = Redis(**app.config.get('redis_conn', {}))

    sentry = False
    if app.config.get('sentry'):
        try:
            from raven.contrib.flask import Sentry
            sentry = Sentry()
        except Exception:
            app.logger.error("Unable to initialize sentry!")

    # Helpful global vars
    # =======================================================================
    app.SATOSHI = Decimal('0.00000001')
    app.MAX_DECIMALS = 28

    # Configure app for running manage.py functions
    # =======================================================================
    if mode == "manage":
        # Initialize the migration settings
        Migrate(app, db)
        # Disable for management mode
        if sentry:
            sentry = False

    # Configure app for serving web content
    # =======================================================================
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

    # Configure app for running scheduler.py functions + instantiate scheduler
    # =======================================================================
    elif mode == "scheduler":
        if sentry and 'SENTRY_NAME' in app.config:
            app.config['SENTRY_NAME'] = app.config['SENTRY_NAME'] + "_scheduler"

        app.logger.info("=" * 80)
        app.logger.info("SimpleCoin cron scheduler starting up...")
        setproctitle.setproctitle("simplecoin_scheduler")

        # Make app accessible from out monkey patched code. Messy....
        ThreadPool.app = app
        sched = Scheduler(standalone=True)
        # monkey patch the thread pool for flask contexts
        ThreadPool._old_run_jobs = ThreadPool._run_jobs
        def _run_jobs(self, core):
            self.app.logger.debug("Starting patched threadpool worker!")
            with self.app.app_context():
                ThreadPool._old_run_jobs(self, core)
        ThreadPool._run_jobs = _run_jobs
        # All these tasks actually change the database, and shouldn't
        # be run by the staging server
        if not app.config.get('stage', False):
            # every minute at 55 seconds after the minute
            sched.add_cron_job(sch.generate_credits, second=55)
            sched.add_cron_job(sch.create_trade_req, args=("sell",), minute=1,
                               hour="0,6,12,18")
            sched.add_cron_job(sch.create_trade_req, args=("buy",), minute=1,
                               hour="0,6,12,18")
            # every minute at 55 seconds after the minute
            sched.add_cron_job(sch.collect_minutes, second=35)
            sched.add_cron_job(sch.collect_ppagent_data, second=40)
            # every five minutes 20 seconds after the minute
            sched.add_cron_job(sch.compress_minute,
                               minute='0,5,10,15,20,25,30,35,40,45,50,55',
                               second=20)
            # every hour 2.5 minutes after the hour
            sched.add_cron_job(sch.compress_five_minute, minute=2, second=30)
            # every minute 2 seconds after the minute
            sched.add_cron_job(sch.update_block_state, second=2)
            # every day
            sched.add_cron_job(sch.update_block_state, hour=0, second=0, minute=3)
        else:
            app.logger.info("Stage mode has been set in the configuration, not "
                            "running scheduled database altering cron tasks")

        sched.add_cron_job(sch.update_online_workers,
                           minute='0,5,10,15,20,25,30,35,40,45,50,55',
                           second=30)
        sched.add_cron_job(sch.cache_user_donation, minute='0,15,30,45',
                           second=15)
        sched.add_cron_job(sch.server_status, second=15)
        # every 15 minutes 2 seconds after the minute
        sched.add_cron_job(sch.leaderboard,
                           minute='0,5,10,15,20,25,30,35,40,45,50,55',
                           second=30)

        app.scheduler = sched

    if sentry:
        sentry.init_app(app, logging=True, level=logging.ERROR)

    # Route registration
    # =======================================================================
    from . import views, models, api, rpc_views
    app.register_blueprint(views.main)
    app.register_blueprint(rpc_views.rpc_views)
    app.register_blueprint(api.api, url_prefix='/api')

    return app


def create_manage_app(**kwargs):
    app = create_app("manage", **kwargs)

    return app


from .config import CurrencyKeeper, PowerPoolKeeper, AlgoKeeper, ChainKeeper, LocationKeeper, \
    ConfigChecker
import simplecoin.scheduler as sch
