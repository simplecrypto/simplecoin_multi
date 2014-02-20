import os
from flask.ext.script import Manager
from simpledoge import create_app

app = create_app()
manager = Manager(app)

root = os.path.abspath(os.path.dirname(__file__) + '/../')

from simpledoge import db
from simpledoge.tasks import add_share

from flask import current_app


@manager.command
def init_db():
    with app.app_context():
        db.session.commit()
        db.drop_all()
        db.create_all()


@manager.command
def power_shares():
    for i in xrange(2000000):
        add_share.delay('mrCuJ1WNXGpcBd8FA6H2cSeQLLXYuJ3qVt', 16)


@manager.command
def runserver():
    current_app.run(debug=True, host='0.0.0.0')


if __name__ == "__main__":
    manager.run()
