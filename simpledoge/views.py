from flask import render_template, Blueprint, abort

import yaml


main = Blueprint('main', __name__)


@main.route("/")
def home():
    return render_template('home.html')


@main.route("/<address>")
def view_resume(address=None):

    return render_template('user_stats.html')
