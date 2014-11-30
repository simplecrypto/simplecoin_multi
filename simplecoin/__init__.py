import subprocess
import logging
import os
import toml
import socket
import sys
import setproctitle
import inspect
try:
    import cdecimal
    sys.modules["decimal"] = cdecimal
except ImportError:
    pass

from apscheduler.threadpool import ThreadPool
from apscheduler.scheduler import Scheduler
from decimal import Decimal
from redis import Redis
from flask import Flask, current_app
from flask.ext.cache import Cache
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.migrate import Migrate
from flask.ext.babel import Babel
from jinja2 import FileSystemLoader
from werkzeug.local import LocalProxy

import simplecoin.filters as filters


socket.setdefaulttimeout(10)


root = os.path.abspath(os.path.dirname(__file__) + '/../')
db = SQLAlchemy()
cache = Cache()
babel = Babel()
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


def create_app(mode, configs=None, log_level=None, **kwargs):
    # Allow configuration information to be specified with enviroment vars
    env_configs = {}
    for key in os.environ:
        if key.startswith('SIMPLECOIN_CONFIG'):
            env_configs[key] = os.environ[key]

    env_configs = [env_configs[value] for value in sorted(env_configs)]

    configs = ['defaults.toml'] + (env_configs or []) + (configs or [])
    if len(configs) == 1:
        print("Unable to start with only the default config values! {}"
              .format(configs))
        exit(2)

    config_vars = {}
    for config in configs:
        if isinstance(config, basestring):
            if os.path.isabs(config):
                config_path = config
            else:
                config_path = os.path.join(root, config)
            config = open(config_path)

        updates = toml.loads(config.read())
        toml.toml_merge_dict(config_vars, updates)

    # Initialize our flask application
    # =======================================================================
    app = Flask(__name__, static_folder='../static', static_url_path='/static')
    app.jinja_loader = FileSystemLoader(os.path.join(root, 'templates'))

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

    # Register the powerpool datastore + Cache
    # =======================================================================
    db.init_app(app)
    babel.init_app(app)
    app.config['BABEL_DEFAULT_LOCALE'] = app.config.get('default_locale')

    def configure_redis(config):
        typ = config.pop('type')
        if typ == "mock_redis":
            from mockredis import mock_redis_client
            return mock_redis_client()
        return Redis(**config)

    cache_config = app.config.get('main_cache', dict(type='live'))
    cache_redis = configure_redis(cache_config)

    ds_config = app.config.get('redis_conn', dict(type='live'))
    ds_redis = configure_redis(ds_config)

    # Take advantage of the fact that werkzeug lets the host kwargs be a Redis
    # compatible object
    cache.init_app(app, config=dict(CACHE_TYPE='redis', CACHE_REDIS_HOST=cache_redis))
    app.redis = ds_redis

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
    if mode == "manage" or mode == "webserver":
        # Dynamically add all the filters in the filters.py file
        for name, func in inspect.getmembers(filters, inspect.isfunction):
            app.jinja_env.filters[name] = func

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

        for task_config in app.config['tasks']:
            if not task_config.get('enabled', False):
                continue

            stripped_config = task_config.copy()
            del stripped_config['enabled']
            task = getattr(sch, task_config['name'])
            sched.add_cron_job(task, **stripped_config)

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


from .config import (CurrencyKeeper, PowerPoolKeeper, AlgoKeeper, ChainKeeper,
                     LocationKeeper, ConfigChecker)
import simplecoin.scheduler as sch
