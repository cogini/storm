import os

from storm.database import create_database

from tests.twisted.base import DeferredStoreTest, StorePoolTest

from twisted.trial.unittest import TestCase


class PostgresDeferredStoreTest(TestCase, DeferredStoreTest):

    def setUp(self):
        return DeferredStoreTest.setUp(self)

    def tearDown(self):
        return DeferredStoreTest.tearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    def create_tables(self):
        connection = self.connection
        connection.execute("CREATE TABLE foo "
                           "(id SERIAL PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE bar "
                           "(id SERIAL PRIMARY KEY,"
                           " foo_id INTEGER, title VARCHAR)")
        connection.execute("CREATE TABLE egg "
                           "(id SERIAL PRIMARY KEY, value INTEGER)")


class PostgresStorePoolTest(TestCase, StorePoolTest):

    def setUp(self):
        return StorePoolTest.setUp(self)

    def tearDown(self):
        return StorePoolTest.tearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_POSTGRES_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_POSTGRES_URI"])

    def create_tables(self):
        connection = self.connection
        connection.execute("CREATE TABLE foo "
                           "(id SERIAL PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE bar "
                           "(id SERIAL PRIMARY KEY,"
                           " foo_id INTEGER, title VARCHAR)")
