from flask.ext.sqlalchemy import (_BoundDeclarativeMeta, BaseQuery,
                                  _QueryProperty)
from sqlalchemy.ext.declarative import declarative_base

from . import db


class BaseMapper(object):
    # Allows us to run query on the class directly, instead of through a
    # session
    query_class = BaseQuery
    query = None

    standard_join = []


# setup our base mapper and database metadata
metadata = db.MetaData()
base = declarative_base(cls=BaseMapper,
                        metaclass=_BoundDeclarativeMeta,
                        metadata=metadata,
                        name='Model')
base.query = _QueryProperty(db)
db.Model = base
