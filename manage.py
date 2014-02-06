from flask.ext.script import Manager
from simpledoge import create_app

app = create_app()
manager = Manager(app)

import sqlalchemy

from simpledoge import db

from flask import current_app


@manager.command
def init_db():
    try:
        db.engine.execute("CREATE EXTENSION hstore")
    except sqlalchemy.exc.ProgrammingError as e:
        if 'already exists' in str(e):
            pass
        else:
            raise Exception("Unable to enable HSTORE extension")
    db.session.commit()
    db.drop_all()
    db.create_all()


@manager.command
def runserver():
    current_app.run(debug=True, host='0.0.0.0')


if __name__ == "__main__":
    manager.run()
