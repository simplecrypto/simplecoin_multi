import os
from flask.ext.script import Manager
from simpledoge import create_app

app = create_app()
manager = Manager(app)

root = os.path.abspath(os.path.dirname(__file__) + '/../')

from simpledoge import db

from flask import current_app


@manager.command
def init_db():
    with app.app_context():
        db.session.commit()
        db.drop_all()
        db.create_all()


@manager.command
def provision():
    from simpledoge.provision import provision
    provision()


@manager.command
def generate_trans():
    """ Generates testing database fixtures """
    init_db()
    provision()
    os.system("pg_dump -c -U simpledoge -h localhost simpledoge -f " + root + "/assets/test_provision.sql")


@manager.command
def runserver():
    current_app.run(debug=True, host='0.0.0.0')


if __name__ == "__main__":
    manager.run()
