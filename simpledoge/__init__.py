from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from jinja2 import FileSystemLoader

import logging
import six
import os
import yaml


root = os.path.abspath(os.path.dirname(__file__) + '/../')
db = SQLAlchemy()


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

    # add the debug toolbar if we're in debug mode...
    if app.config['DEBUG']:
        from flask_debugtoolbar import DebugToolbarExtension
        DebugToolbarExtension(app)
        app.logger.handlers[0].setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s '
            '[in %(filename)s:%(lineno)d]'))

    # register all our plugins
    db.init_app(app)

    from .tasks import celery
    celery.conf.update(app.config)

    # Route registration
    # =========================================================================
    from . import views, models
    app.register_blueprint(views.main)

    return app
