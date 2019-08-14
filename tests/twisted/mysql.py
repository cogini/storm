import os

from storm.database import create_database

from tests.twisted.base import DeferredStoreTest, StorePoolTest

from twisted.trial.unittest import TestCase


class MySQLDeferredStoreTest(TestCase, DeferredStoreTest):

    def setUp(self):
        return DeferredStoreTest.setUp(self)

    def tearDown(self):
        return DeferredStoreTest.tearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_MYSQL_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_MYSQL_URI"])

    def create_tables(self):
        connection = self.connection
        connection.execute("CREATE TABLE foo "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " title VARCHAR(50) DEFAULT 'Default Title')"
                           " ENGINE=InnoDB")
        connection.execute("CREATE TABLE bar "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " foo_id INTEGER, title VARCHAR(50))"
                           " ENGINE=InnoDB")
        connection.execute("CREATE TABLE egg "
                           "(id INT PRIMARY KEY AUTO_INCREMENT, value INTEGER)"
                           " ENGINE=InnoDB")


class MySQLStorePoolTest(TestCase, StorePoolTest):

    def setUp(self):
        return StorePoolTest.setUp(self)

    def tearDown(self):
        return StorePoolTest.tearDown(self)

    def is_supported(self):
        return bool(os.environ.get("STORM_MYSQL_URI"))

    def create_database(self):
        self.database = create_database(os.environ["STORM_MYSQL_URI"])

    def create_tables(self):
        connection = self.connection
        connection.execute("CREATE TABLE foo "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " title VARCHAR(50) DEFAULT 'Default Title')"
                           " ENGINE=InnoDB")
        connection.execute("CREATE TABLE bar "
                           "(id INT PRIMARY KEY AUTO_INCREMENT,"
                           " foo_id INTEGER, title VARCHAR(50))"
                           " ENGINE=InnoDB")
