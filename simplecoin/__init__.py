from flask import Flask, current_app
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.cache import Cache
from jinja2 import FileSystemLoader
from werkzeug.local import LocalProxy
from bitcoinrpc import AuthServiceProxy
from datetime import datetime

import subprocess
import logging
import os
import yaml


root = os.path.abspath(os.path.dirname(__file__) + '/../')
db = SQLAlchemy()
cache = Cache()
coinserv = LocalProxy(
    lambda: getattr(current_app, 'rpc_connection', None))


def create_app(config='/config.yml', celery=False):
    # initialize our flask application
    app = Flask(__name__, static_folder='../static', static_url_path='/static')

    # set our template path and configs
    app.jinja_loader = FileSystemLoader(os.path.join(root, 'templates'))
    config_vars = yaml.load(open(root + config))
    # inject all the yaml configs
    app.config.update(config_vars)
    app.logger.info(app.config)

    app.rpc_connection = AuthServiceProxy(
        "http://{0}:{1}@{2}:{3}/"
        .format(app.config['coinserv']['username'],
                app.config['coinserv']['password'],
                app.config['coinserv']['address'],
                app.config['coinserv']['port'],
                pool_kwargs=dict(maxsize=app.config.get('maxsize', 10))))

    # add the debug toolbar if we're in debug mode...
    if app.config['DEBUG']:
        from flask_debugtoolbar import DebugToolbarExtension
        DebugToolbarExtension(app)
        app.logger.handlers[0].setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s '
            '[in %(filename)s:%(lineno)d]'))

    # register all our plugins
    db.init_app(app)
    cache_config = {'CACHE_TYPE': 'redis'}
    cache_config.update(app.config.get('main_cache', {}))
    cache.init_app(app, config=cache_config)

    if not celery:
        hdlr = logging.FileHandler(app.config.get('log_file', 'webserver.log'))
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        app.logger.addHandler(hdlr)
        app.logger.setLevel(logging.INFO)

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


    # filters for jinja
    @app.template_filter('time_ago')
    def pretty_date(time=False):
        """
        Get a datetime object or a int() Epoch timestamp and return a
        pretty string like 'an hour ago', 'Yesterday', '3 months ago',
        'just now', etc
        """

        now = datetime.utcnow()
        if type(time) is int:
            diff = now - datetime.fromtimestamp(time)
        elif isinstance(time, datetime):
            diff = now - time
        elif not time:
            diff = now - now
        second_diff = diff.seconds
        day_diff = diff.days

        if day_diff < 0:
            return ''

        if day_diff == 0:
            if second_diff < 60:
                return str(second_diff) + " seconds ago"
            if second_diff < 120:
                return "a minute ago"
            if second_diff < 3600:
                return str(second_diff / 60) + " minutes ago"
            if second_diff < 7200:
                return "an hour ago"
            if second_diff < 86400:
                return str(second_diff / 3600) + " hours ago"
        if day_diff == 1:
            return "Yesterday"
        if day_diff < 7:
            return str(day_diff) + " days ago"
        if day_diff < 31:
            return str(day_diff/7) + " weeks ago"
        if day_diff < 365:
            return str(day_diff/30) + " months ago"
        return str(day_diff/365) + " years ago"

    from .tasks import celery
    celery.conf.update(app.config)

    # Route registration
    # =========================================================================
    from . import views, models, api, rpc_views
    app.register_blueprint(views.main)
    app.register_blueprint(api.api, url_prefix='/api')

    return app
