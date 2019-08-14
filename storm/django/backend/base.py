__metaclass__ = type

__all__ = [
    'DatabaseWrapper', 'DatabaseError', 'IntegrityError',
    ]

from django.conf import settings
import transaction

from storm.django.stores import get_store, get_store_uri
from storm.exceptions import DatabaseError, IntegrityError


class StormDatabaseWrapperMixin(object):

    _store = None

    def _get_connection(self):
        if self._store is None:
            self._store = get_store(settings.DATABASE_NAME)
        # Make sure that the store is registered with the transaction
        # manager: we don't know what the connection will be used for.
        self._store._event.emit("register-transaction")
        self._store._connection._ensure_connected()
        return self._store._connection._raw_connection

    def _set_connection(self, connection):
        # Ignore attempts to set the connection.
        pass

    connection = property(_get_connection, _set_connection)

    def _valid_connection(self):
        # Storm handles the connection liveness checks.
        return True

    def _cursor(self, *args):
        cursor = super(StormDatabaseWrapperMixin, self)._cursor(*args)
        return StormCursorWrapper(self._store, cursor)

    def _commit(self):
        #print "commit"
        try:
            transaction.commit()
        except Exception:
            transaction.abort()
            raise

    def _rollback(self):
        #print "rollback"
        transaction.abort()

    def close(self):
        # As we are borrowing Storm's connection, we shouldn't close
        # it behind Storm's back.
        self._store = None


class StormCursorWrapper(object):
    """A cursor wrapper that checks for disconnection errors."""

    def __init__(self, store, cursor):
        self._connection = store._connection
        self._cursor = cursor

    def _check_disconnect(self, *args, **kwargs):
        from django.db import DatabaseError as DjangoDatabaseError
        kwargs['extra_disconnection_errors'] = DjangoDatabaseError
        return self._connection._check_disconnect(*args, **kwargs)

    def execute(self, statement, *args):
        """Execute an SQL statement."""
        return self._check_disconnect(self._cursor.execute, statement, *args)

    def fetchone(self):
        """Fetch one row from the result."""
        return self._check_disconnect(self._cursor.fetchone)

    def fetchall(self):
        """Fetch all rows from the result."""
        return self._check_disconnect(self._cursor.fetchall)

    def fetchmany(self, *args):
        """Fetch multiple rows from the result."""
        return self._check_disconnect(self._cursor.fetchmany, *args)

    @property
    def description(self):
        """Fetch the description of the result."""
        return self._check_disconnect(getattr, self._cursor, "description")

    @property
    def rowcount(self):
        """Fetch the number of rows in the result."""
        return self._check_disconnect(getattr, self._cursor, "rowcount")

    @property
    def query(self):
        """Fetch the last executed query."""
        return self._check_disconnect(getattr, self._cursor, "query")


PostgresStormDatabaseWrapper = None
MySQLStormDatabaseWrapper = None


def DatabaseWrapper(*args, **kwargs):
    store_uri = get_store_uri(settings.DATABASE_NAME)

    # Create a DatabaseWrapper class that uses an underlying Storm
    # connection.  We don't support sqlite here because Django expects
    # a bunch of special setup on the connection that Storm doesn't
    # do.
    if store_uri.startswith('postgres:'):
        global PostgresStormDatabaseWrapper
        if PostgresStormDatabaseWrapper is None:
            from django.db.backends.postgresql_psycopg2.base import (
                DatabaseWrapper as PostgresDatabaseWrapper)
            class PostgresStormDatabaseWrapper(StormDatabaseWrapperMixin,
                                               PostgresDatabaseWrapper):
                pass
        DatabaseWrapper = PostgresStormDatabaseWrapper
    elif store_uri.startswith('mysql:'):
        global MySQLStormDatabaseWrapper
        if MySQLStormDatabaseWrapper is None:
            from django.db.backends.mysql.base import (
                DatabaseWrapper as MySQLDatabaseWrapper)
            class MySQLStormDatabaseWrapper(StormDatabaseWrapperMixin,
                                            MySQLDatabaseWrapper):
                pass
        DatabaseWrapper = MySQLStormDatabaseWrapper
    else:
        assert False, (
            "Unsupported database backend: %s" % store_uri)

    return DatabaseWrapper(*args, **kwargs)
