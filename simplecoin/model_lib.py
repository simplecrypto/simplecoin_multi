from decimal import Decimal
from flask.ext.sqlalchemy import (_BoundDeclarativeMeta, BaseQuery,
                                  _QueryProperty)
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.types as types

from . import db


class SqliteNumeric(types.TypeDecorator):
    """ Replaces the regular Numeric column and stores decimals as strings
    if we're using SQLite """
    impl = types.Numeric

    def __init__(self, *args, **kwargs):
        if self.impl == types.Numeric:
            kwargs['scale'] = 28
            kwargs['precision'] = 1000
        types.TypeDecorator.__init__(self, *args, **kwargs)

    def load_dialect_impl(self, dialect):
        if dialect.name == "sqlite":
            self.impl = dialect.type_descriptor(types.VARCHAR)
        else:
            # Class attribute -> instance attribute
            self.impl = self.impl
        return self.impl

    def process_bind_param(self, value, dialect):
        if dialect.name == "sqlite":
            if isinstance(value, Decimal):
                return str(value)
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == "sqlite":
            if value is None:
                return None
            return Decimal(value)
        return value
db.Numeric = SqliteNumeric


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
