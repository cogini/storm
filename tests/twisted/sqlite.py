from storm.databases.sqlite import SQLite
from storm.uri import URI

from tests.twisted.base import DeferredStoreTest, StorePoolTest
from tests.helper import TestHelper, MakePath

from twisted.trial.unittest import TestCase


class SQLiteDeferredStoreTest(TestCase, TestHelper, DeferredStoreTest):

    helpers = [MakePath]

    def setUp(self):
        TestHelper.setUp(self)
        return DeferredStoreTest.setUp(self)

    def tearDown(self):
        def cb(passthrough):
            TestHelper.tearDown(self)
            return passthrough
        return DeferredStoreTest.tearDown(self).addBoth(cb)

    def create_database(self):
        self.database = SQLite(URI("sqlite:%s?synchronous=OFF" %
                                   self.make_path()))

    def create_tables(self):
        connection = self.connection
        connection.execute("CREATE TABLE foo "
                           "(id INTEGER PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE bar "
                           "(id INTEGER PRIMARY KEY,"
                           " foo_id INTEGER, title VARCHAR)")
        connection.execute("CREATE TABLE egg "
                           "(id INTEGER PRIMARY KEY, value INTEGER)")


class SQLiteStorePoolTest(TestCase, TestHelper, StorePoolTest):

    helpers = [MakePath]

    def setUp(self):
        TestHelper.setUp(self)
        return StorePoolTest.setUp(self)

    def tearDown(self):
        def cb(passthrough):
            TestHelper.tearDown(self)
            return passthrough
        return StorePoolTest.tearDown(self).addBoth(cb)

    def create_database(self):
        self.database = SQLite(URI("sqlite:%s?synchronous=OFF" %
                                   self.make_path()))

    def create_tables(self):
        connection = self.connection
        connection.execute("CREATE TABLE foo "
                           "(id INTEGER PRIMARY KEY,"
                           " title VARCHAR DEFAULT 'Default Title')")
        connection.execute("CREATE TABLE bar "
                           "(id INTEGER PRIMARY KEY,"
                           " foo_id INTEGER, title VARCHAR)")
