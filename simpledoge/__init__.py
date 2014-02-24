from flask import Flask, current_app
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.cache import Cache
from jinja2 import FileSystemLoader
from werkzeug.local import LocalProxy
from bitcoinrpc import AuthServiceProxy

import logging
import six
import os
import yaml


root = os.path.abspath(os.path.dirname(__file__) + '/../')
db = SQLAlchemy()
cache = Cache()
coinserv = LocalProxy(
    lambda: getattr(current_app, 'rpc_connection', None))


def create_app(config='/config.yml'):
    # initialize our flask application
    app = Flask(__name__, static_folder='../static', static_url_path='/static')

    # set our template path and configs
    app.jinja_loader = FileSystemLoader(os.path.join(root, 'templates'))
    config_vars = yaml.load(open(root + config))
    # merge the public and private keys
    public = list(six.iteritems(config_vars['public']))
    private = list(six.iteritems(config_vars['private']))
    config_vars = dict(private + public)
    for key, val in config_vars.items():
        app.config[key] = val

    app.rpc_connection = AuthServiceProxy(
        "http://{0}:{1}@{2}:{3}/"
        .format(app.config['coinserv']['username'],
                app.config['coinserv']['password'],
                app.config['coinserv']['address'],
                app.config['coinserv']['port']))

    # add the debug toolbar if we're in debug mode...
    if app.config['DEBUG']:
        from flask_debugtoolbar import DebugToolbarExtension
        DebugToolbarExtension(app)
        app.logger.handlers[0].setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s '
            '[in %(filename)s:%(lineno)d]'))

    # register all our plugins
    db.init_app(app)
    cache.init_app(app, config={'CACHE_TYPE': 'simple'})

    from .tasks import celery
    celery.conf.update(app.config)

    # Route registration
    # =========================================================================
    from . import views, models, api
    app.register_blueprint(views.main)
    app.register_blueprint(api.api, url_prefix='/api')

    return app
