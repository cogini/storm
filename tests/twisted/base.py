#
# Copyright (c) 2007 Canonical
# Copyright (c) 2007 Thomas Herve <thomas@nimail.org>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

"""
Test for twistorm.
"""

from storm.properties import Int, Unicode
from storm.expr import Count
from storm.references import Reference
from storm.exceptions import OperationalError, ThreadSafetyError

from storm.twisted.store import (
        DeferredStore, StoreThread, StorePool, AlreadyStopped )
from storm.twisted.wrapper import DeferredReference, DeferredReferenceSet

from twisted.trial.unittest import TestCase
from twisted.internet.defer import gatherResults, deferredGenerator
from twisted.internet.defer import waitForDeferred, DeferredList
from twisted.internet.defer import succeed, fail


class Foo(object):
    """
    Test table.
    """
    __storm_table__ = "foo"
    id = Int(primary=True)
    title = Unicode()



class Bar(object):
    """
    Test table referencing to C{Foo}
    """
    __storm_table__ = "bar"
    id = Int(primary=True)
    title = Unicode()
    foo_id = Int()
    foo = DeferredReference(foo_id, Foo.id)



class FooRefSet(Foo):
    """
    A C{Foo} class with a C{DeferredReferenceSet} to get all the bars related.
    """
    bars = DeferredReferenceSet(Foo.id, Bar.foo_id)



class FooRefSetOrderID(Foo):
    """
    A C{Foo} class with an order C{DeferredReferenceSet} to C{Bar}.
    """
    bars = DeferredReferenceSet(Foo.id, Bar.foo_id, order_by=Bar.id)



class Egg(object):
    """
    Test table.
    """
    __storm_table__ = "egg"
    id = Int(primary=True)
    value = Int()



class DeferredStoreTest(object):
    """
    Tests for L{DeferredStore}.
    """

    def setUp(self):
        """
        Create a test sqlite database, and insert some data.
        """
        self.create_database()
        connection = self.connection = self.database.connect()
        self.drop_tables()
        self.create_tables()
        connection.execute("INSERT INTO foo VALUES (10, 'Title 30')")
        connection.execute("INSERT INTO bar VALUES (10, 10, 'Title 50')")
        connection.execute("INSERT INTO bar VALUES (11, 10, 'Title 40')")
        connection.execute("INSERT INTO egg VALUES (1, 4)")
        connection.execute("INSERT INTO egg VALUES (2, 3)")
        connection.execute("INSERT INTO egg VALUES (3, 7)")
        connection.execute("INSERT INTO egg VALUES (4, 5)")
        connection.commit()
        self.store = DeferredStore(self.database)
        return self.store.start()


    def tearDown(self):
        """
        Kill the store (and its underlying thread).
        """
        def _stop(ign):
            return self.store.stop().addCallback(_drop)
        def _drop(ign):
            self.drop_tables()
        return self.store.rollback().addCallback(_stop)


    def create_database(self):
        raise NotImplementedError()


    def create_tables(self):
        raise NotImplementedError()


    def drop_tables(self):
        for table in ["foo", "bar", "egg"]:
            try:
                self.connection.execute("DROP TABLE %s" % table)
                self.connection.commit()
            except:
                self.connection.rollback()


    def test_multiple_start(self):
        """
        Check that start raises an exception when the store is already started.
        """
        self.assertRaises(RuntimeError, self.store.start)


    def test_get(self):
        """
        Try to get an object from the store and check its attributes.
        """
        def cb(result):
            self.assertEquals(result.title, u"Title 30")
            self.assertEquals(result.id, 10)
        return self.store.get(Foo, 10).addCallback(cb)


    def test_add(self):
        """
        Add an object to the store.
        """
        foo = Foo()
        foo.title = u"Great title"
        foo.id = 11
        def cb_add(ign):
            return self.store.get(Foo, 11).addCallback(cb_get)
        def cb_get(result):
            self.assertEquals(result.title, u"Great title")
            self.assertEquals(result.id, 11)
        return self.store.add(foo).addCallback(cb_add)


    def test_add_default_value(self):
        """
        When adding an object to the store, the default values from the
        database are retrieved and put into the object.
        """
        foo = Foo()
        foo.id = 11
        def cb_add(result):
            self.assertIdentical(result, None)
            return self.store.get(Foo, 11).addCallback(cb_get)
        def cb_get(result):
            self.assertEquals(result.title, u"Default Title")
            self.assertEquals(result.id, 11)
        return self.store.add(foo).addCallback(cb_add)


    def test_execute(self):
        """
        Test a direct execute on the store, and the C{get_one} method of
        C{DeferredResult}.
        """
        def cb_execute(result):
            return result.get_one().addCallback(cb_result)
        def cb_result(result):
            self.assertEquals(result, (u"Title 30",))
        return self.store.execute("SELECT title FROM foo WHERE id=10"
            ).addCallback(cb_execute)


    def test_execute_all(self):
        """
        Test a direct execute on the store, and the C{all} method of
        C{DeferredResult}.
        """
        def cb_execute(result):
            return result.get_all().addCallback(cb_result)
        def cb_result(result):
            self.assertEquals(result, [(u"Title 50",), (u"Title 40",)])
        return self.store.execute("SELECT title FROM bar"
            ).addCallback(cb_execute)


    def test_remove(self):
        """
        Trying removing an object from the database.
        """
        def cb_get(result):
            return self.store.remove(result).addCallback(cb_remove)
        def cb_remove(ign):
            return self.store.get(Foo, 10).addCallback(cb_get_after_remove)
        def cb_get_after_remove(result):
            self.assertIdentical(result, None)
        return self.store.get(Foo, 10).addCallback(cb_get)


    def test_find(self):
        """
        Try to find a list of objects using the store.
        """
        def cb_find(results):
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 2)
            titles = [results[0].title, results[1].title]
            titles.sort()
            self.assertEquals(titles, [u"Title 40", u"Title 50"])
        return self.store.find(Bar).addCallback(cb_find)


    def test_find_first(self):
        """
        Try to get the first object matching a query.
        """
        def cb_find(results):
            results.order_by(Bar.title)
            return results.first().addCallback(cb_all)
        def cb_all(result):
            self.assertEquals(result.title, u"Title 40")
            self.assertEquals(result.id, 11)
        return self.store.find(Bar).addCallback(cb_find)


    def test_find_last(self):
        """
        Try to get the last object matching a query.
        """
        def cb_find(results):
            results.order_by(Bar.title)
            return results.last().addCallback(cb_all)
        def cb_all(result):
            self.assertEquals(result.title, u"Title 50")
            self.assertEquals(result.id, 10)
        return self.store.find(Bar).addCallback(cb_find)


    def test_find_any(self):
        """
        Try to get an object matching a query using the C{any} method.
        """
        def cb_find(results):
            return results.any().addCallback(cb_all)
        def cb_all(result):
            self.assertEquals(result.title, u"Title 50")
            self.assertEquals(result.id, 10)
        return self.store.find(Bar).addCallback(cb_find)


    def test_find_max(self):
        """
        Try to get the maximum of a value after a find.
        """
        def cb_find(results):
            return results.max(Egg.value).addCallback(cb_all)
        def cb_all(result):
            self.assertEquals(result, 7)
        return self.store.find(Egg).addCallback(cb_find)


    def test_find_min(self):
        """
        Try to get the minimum of a value after a find.
        """
        def cb_find(results):
            return results.min(Egg.value).addCallback(cb_all)
        def cb_all(result):
            self.assertEquals(result, 3)
        return self.store.find(Egg).addCallback(cb_find)


    def test_find_avg(self):
        """
        Try to get the average of a value after a find.
        """
        def cb_find(results):
            return results.avg(Egg.value).addCallback(cb_all)
        def cb_all(result):
            self.assertEquals(result, 4.75)
        return self.store.find(Egg).addCallback(cb_find)


    def test_find_sum(self):
        """
        Try to get the sum of a value after a find.
        """
        def cb_find(results):
            return results.sum(Egg.value).addCallback(cb_all)
        def cb_all(result):
            self.assertEquals(result, 19)
        return self.store.find(Egg).addCallback(cb_find)


    def test_find_count(self):
        """
        Try to get the count of a result after a find.
        """
        def cb_find(results):
            return results.count().addCallback(cb_all)
        def cb_all(result):
            self.assertEquals(result, 2)
        return self.store.find(Egg, Egg.value >= 5).addCallback(cb_find)


    def test_find_remove(self):
        """
        Remove the result of a find query.
        """
        def cb_find(results):
            return results.remove().addCallback(cb_remove)
        def cb_remove(ignore):
            return self.store.find(Egg).addCallback(cb_find_after_remove)
        def cb_find_after_remove(results):
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 2)
        return self.store.find(Egg, Egg.value >= 5).addCallback(cb_find)


    def test_find_limit(self):
        """
        Put a limit on the number of results of a find.
        """
        def cb_find(results):
            results.config(limit=3)
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 3)
        return self.store.find(Egg).addCallback(cb_find)


    def test_find_union(self):
        """
        Call C{union} on 2 differents C{DeferredResultSet}.
        """
        def cb_find(results):
            result1, result2 = results
            results = result1.union(result2._resultSet)
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 3)
        d1 = self.store.find(Egg, Egg.value >= 5)
        d2 = self.store.find(Egg, Egg.value == 3)
        return gatherResults([d1, d2]).addCallback(cb_find)


    def test_find_difference(self):
        """
        Call C{union} on 2 differents C{DeferredResultSet}.
        """
        if self.__class__.__name__.startswith("MySQL"):
            return
        def cb_find(results):
            result1, result2 = results
            results = result1.difference(result2._resultSet)
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 1)
            self.assertEquals(results[0].value, 5)
        d1 = self.store.find(Egg, Egg.value >= 5)
        d2 = self.store.find(Egg, Egg.value == 7)
        return gatherResults([d1, d2]).addCallback(cb_find)


    def test_find_intersection(self):
        """
        Call C{intersection} on 2 differents C{DeferredResultSet}.
        """
        if self.__class__.__name__.startswith("MySQL"):
            return
        def cb_find(results):
            result1, result2 = results
            results = result1.intersection(result2._resultSet)
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 1)
            self.assertEquals(results[0].value, 7)
        d1 = self.store.find(Egg, Egg.value >= 5)
        d2 = self.store.find(Egg, Egg.value == 7)
        return gatherResults([d1, d2]).addCallback(cb_find)


    def test_find_values(self):
        """
        Filter the fields returned by a find using the values method.
        """
        def cb_find(results):
            return results.values(Bar.title).addCallback(cb_all)
        def cb_all(titles):
            titles.sort()
            self.assertEquals(titles, [u"Title 40", u"Title 50"])
        return self.store.find(Bar).addCallback(cb_find)


    def test_find_and_set(self):
        """
        The C{set} method of a C{ResultSet} should update the specified fields
        in a thread.
        """
        def cb_find(results):
            return results.set(title=u"Title").addCallback(cb_set, results)
        def cb_set(ignore, results):
            return results.values(Bar.title).addCallback(cb_all)
        def cb_all(titles):
            titles.sort()
            self.assertEquals(titles, [u"Title", u"Title"])
        return self.store.find(Bar).addCallback(cb_find)


    def test_find_offset(self):
        """
        Put an offset on the number of results of a find.
        """
        def cb_find(results):
            results.config(offset=2)
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 2)
        return self.store.find(Egg).addCallback(cb_find)


    def test_find_offset_limit(self):
        """
        Put an offset and limit in the number of results of a find.
        """
        def cb_find(results):
            results.config(offset=1, limit=2)
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 2)
        return self.store.find(Egg).addCallback(cb_find)


    @deferredGenerator
    def test_find_defgen(self):
        """
        Do a find, add an object, then do another find: this to ensure that the
        connection remains in the dedicated thread.
        """
        d = self.store.find(Bar)
        wfd = waitForDeferred(d)
        yield wfd
        results = wfd.getResult()
        d =  results.all()
        wfd = waitForDeferred(d)
        yield wfd
        wfd.getResult()
        foo = Foo()
        foo.title = u"Great title"
        foo.id = 11
        d = self.store.add(foo)
        wfd = waitForDeferred(d)
        yield wfd
        wfd.getResult()
        d = self.store.find(Foo)
        wfd = waitForDeferred(d)
        yield wfd
        results = wfd.getResult()


    def test_find_order_by(self):
        """
        Try to find a list of objects using the store, then order the result
        set.
        """
        def cb_find(results):
            results.order_by(Bar.title)
            return results.all().addCallback(cb_all)
        def cb_all(results):
            self.assertEquals(len(results), 2)
            titles = [results[0].title, results[1].title]
            self.assertEquals(titles, [u"Title 40", u"Title 50"])
        return self.store.find(Bar).addCallback(cb_find)


    def test_find_and_rollback(self):
        """
        Accessing an object outside of a transaction fails because the object
        hasn't been resolved yet.
        """
        def cb_find(results):
            results.order_by(Bar.title)
            return results.all().addCallback(cb_all)
        def cb_all(results):
            return self.store.rollback().addCallback(cbRollback, results)
        def cbRollback(ign, results):
            self.assertEquals(len(results), 2)
            self.assertRaises(RuntimeError, getattr, results[0], "title")
        return self.store.find(Bar).addCallback(cb_find)


    def test_find_is_empty(self):
        """
        DeferredReference.is_empty returns a Deferred that fires with True or
        False depending if the matched result set is empty or not.
        """
        def cb_find(results):
            return results.is_empty().addCallback(self.assertEquals, False)

        return self.store.find(Bar).addCallback(cb_find)


    def test_find_group_by(self):
        """
        DeferredReference.group_by is a simple wrapper to the group_by method
        of the reference set.
        """
        def cb_find(results):
            results.group_by(Bar.foo_id)
            return results.all().addCallback(check)

        def check(result):
            self.assertEquals(result, [(2, 10)])

        return self.store.find((Count(Bar.id), Bar.foo_id)
            ).addCallback(cb_find)


    def test_find_having(self):
        """
        DeferredReference.having is a simple wrapper to the having method of
        the reference set.
        """
        connection = self.database.connect()
        connection.execute("INSERT INTO egg VALUES (5, 7)")
        connection.commit()

        def cb_find(results):
            results.group_by(Egg.value)
            results.having(Egg.value >= 5)
            results.order_by(Egg.value)
            return results.all().addCallback(check)

        def check(result):
            self.assertEquals(result, [(1, 5), (2, 7)])

        return self.store.find((Count(Egg.id), Egg.value)
            ).addCallback(cb_find)


    def test_reference(self):
        """
        Trying to get a reference of an object using C{DeferredReference}.
        """
        def cb_getBar(result):
            return result.foo.addCallback(cb_getFoo)
        def cb_getFoo(fooResult):
            return self.store.get(Foo, 10).addCallback(cb_getFooBar, fooResult)
        def cb_getFooBar(result, fooResult):
            self.assertIdentical(fooResult, result)
            # The result should be valid too
            self.assertEquals(fooResult.title, u"Title 30")
        return self.store.get(Bar, 10).addCallback(cb_getBar)


    def test_reference_setting(self):
        """
        Try to set a reference of an object.
        """
        connection = self.database.connect()
        connection.execute("INSERT INTO foo VALUES (20, 'Title 20')")
        connection.commit()
        def cb_getBar(result):
            return self.store.get(Foo, 20).addCallback(cb_getFooBar, result)
        def cb_getFooBar(result, barResult):
            self.assertRaises(RuntimeError, setattr, barResult, "foo", result)
        return self.store.get(Bar, 10).addCallback(cb_getBar)


    def test_reference_set_unordered(self):
        """
        Get a reference set and call various wrapped methods on it.
        """
        # find test
        def cb_find(results):
            return results.all().addCallback(cb_all)

        def cb_all(results):
            self.assertEquals(len(results), 2)
            titles = [results[0].title, results[1].title]
            titles.sort()
            self.assertEquals(titles, [u"Title 40", u"Title 50"])

        def cb_any(result):
            self.assertTrue(result)

        def cb_values(titles):
            titles.sort()
            self.assertEquals(titles, [u"Title 40", u"Title 50"])

        def do_tests(results):
            results = results.bars
            dfrs = [
                results.find().addCallback(cb_find),
                results.any().addCallback(cb_any),
                results.values(Bar.title).addCallback(cb_values),
            ]
            return DeferredList(dfrs)

        return self.store.get(FooRefSet, 10).addCallback(do_tests)


    def test_reference_set_ordered(self):
        """
        A DeferredReferenceSet has a order_by method which returns a Deferred
        firing when the reference set is ordered.
        """
        def do_tests(result):
            dfrs = [
                result.first().addCallback(lambda t:
                        self.assertEquals(t.title, u"Title 40")),
                result.last().addCallback(lambda t:
                        self.assertEquals(t.title, u"Title 50")),
                    ]
            return DeferredList(dfrs)

        def order(results):
            dfr = results.bars.order_by("title")
            return dfr.addCallback(do_tests)

        return self.store.get(FooRefSet, 10).addCallback(order)


    def test_reference_set_add_remove(self):
        """
        A DeferredReferenceSet has a add method with returns a Deferred once
        the object has been added.
        Try to add things from the reference set async.
        """
        def add_one(result):
            bar = Bar()
            bar.title = u"Yeah"
            return result.bars.add(bar).addCallback(remove_one, result, bar)

        def remove_one(add_result, result, bar):
            return result.bars.remove(bar).addCallback(get_all, result)

        def get_all(ignore, result):
            return result.bars.all().addCallback(check)

        def check(result):
            self.assertEquals(len(result), 2)

        return self.store.get(FooRefSet, 10).addCallback(add_one)


    def test_reference_set_clear(self):
        """
        A DeferredReferenceSet has a clear method which removes all elements
        from the reference set and fires the returned Deferred when done.
        """
        def first_cb(result):
            refs = result.bars
            return check_count(refs, 2).addCallback(clear_cb, refs)

        def check_count(ref_set, num):
            return ref_set.count().addCallback(self.assertEquals, num)

        def clear_cb(result, refs):
            return refs.clear().addCallback(lambda x: check_count(refs, 0))

        return self.store.get(FooRefSet, 10).addCallback(first_cb)


    def test_reference_set_one(self):
        """
        Call C{one} on a C{DeferredBoundReference}.
        """
        connection = self.database.connect()
        connection.execute("INSERT INTO foo VALUES (11, 'Title 40')")
        connection.execute("INSERT INTO bar VALUES (20, 11, 'Title 50')")
        connection.commit()
        def cb_get(result):
            return result.bars.one().addCallback(cb_one)
        def cb_one(result):
            return self.store.get(Bar, 20).addCallback(check, result)
        def check(result, expected):
            self.assertIdentical(result, expected)
        return self.store.get(FooRefSet, 11).addCallback(cb_get)


    def test_reference_set_first(self):
        """
        Call C{first} on an ordered C{DeferredBoundReference}.
        """
        def cb_get(result):
            return result.bars.first().addCallback(cb_one)
        def cb_one(result):
            return self.store.get(Bar, 10).addCallback(check, result)
        def check(result, expected):
            self.assertIdentical(result, expected)
        return self.store.get(FooRefSetOrderID, 10).addCallback(cb_get)


    def test_reference_set_last(self):
        """
        Call C{last} on an ordered C{DeferredBoundReference}.
        """
        def cb_get(result):
            return result.bars.last().addCallback(cb_one)
        def cb_one(result):
            return self.store.get(Bar, 11).addCallback(check, result)
        def check(result, expected):
            self.assertIdentical(result, expected)
        return self.store.get(FooRefSetOrderID, 10).addCallback(cb_get)


    def test_commit(self):
        """
        Make some changes and commit them.
        """
        def cb_get(result):
            return self.store.remove(result).addCallback(cb_remove)
        def cb_remove(ign):
            return self.store.commit().addCallback(cb_commit)
        def cb_commit(ign):
            # To be sure the data is no more in the db, the best is to
            # directly connect to the db
            connection = self.database.connect()
            result = connection.execute("SELECT * FROM foo")
            self.assertEquals(list(result), [])
        return self.store.get(Foo, 10).addCallback(cb_get)


    def test_rollback(self):
        """
        Make and some changes and rollback them.
        """
        def cb_get(result):
            return self.store.remove(result).addCallback(cb_remove)
        def cb_remove(ign):
            return self.store.rollback().addCallback(cbRollback)
        def cbRollback(ign):
            connection = self.database.connect()
            result = connection.execute("SELECT * FROM foo")
            self.assertEquals(list(result), [(10, u"Title 30")])
        return self.store.get(Foo, 10).addCallback(cb_get)


    def test_deferred_reference_multithread(self):
        """
        If a store is restarted, the objects in the cache should still be
        usable, in particular an object shouldn't not store a reference to the
        store thread, as it can change.
        """
        def test(ignore):
            # get a bar and retrieve the deferred reference
            def get_foo(bar):
                return bar.foo

            return self.store.find(Bar, Bar.id == 10).addCallback(lambda x:
                    x.one()).addCallback(get_foo)

        def _restart_store(res):
            def stopped(res):
                self.store = DeferredStore(self.database)
                return self.store.start()
            return self.store.stop().addCallback(stopped)

        def check(foo):
            self.assertEquals(foo.id, 10)
            self.assertEquals(foo.title, u"Title 30")

        return test(None
            ).addCallback(_restart_store
            ).addCallback(test
            ).addCallback(check)


    def test_of(self):
        """
        The DeferredStore associated with an object is returned by the static
        C{of} method.
        """
        def cb_get(result):
            store = DeferredStore.of(result)
            self.assertIdentical(self.store, store)

        return self.store.get(Foo, 10).addCallback(cb_get)


    def test_thread_check(self):
        """
        A L{ThreadSafetyError} is raised when attempting to do an unsafe
        operation, like accessing a C{Reference} attribute via a
        C{DeferredStore}.
        """
        class WrongBar(Bar):
            foo_sync = Reference(Bar.foo_id, Foo.id)
        d = self.store.find(WrongBar).addCallback(lambda result: result.all())

        def check(results):
            self.assertRaises(
                ThreadSafetyError, getattr, results[0], "foo_sync")
        return d.addCallback(check)



class StoreThreadTestCase(TestCase):
    """
    Tests for L{StoreThread}.
    """

    def setUp(self):
        """
        Create an instance of C{StoreThread} and start it.
        """
        self.thread = StoreThread()
        self.thread.start()


    def tearDown(self):
        """
        Kill the running thread.
        """
        self.thread.stop()


    def test_defer_after_stop(self):
        """
        Deferring calls after store is stopped raises C{AlreadyStopped}.
        """
        def cb_stop(r):
            self.assertFailure(self.thread.defer_to_thread(lambda f : None),
                    AlreadyStopped)
        return self.thread.stop().addCallback(cb_stop)


    def test_callback(self):
        """
        Fire a simple function in a thread and check its result.
        """
        def testfunc():
            return 1
        return self.thread.defer_to_thread(testfunc
            ).addCallback(self.assertEquals, 1)


    def test_errback(self):
        """
        Raising an exception in a thread returns a failure.
        """
        def testfunc():
            raise RuntimeError("Error!")
        return self.assertFailure(self.thread.defer_to_thread(testfunc),
                                  RuntimeError)



class StorePoolTest(object):
    """
    Tests for L{StorePool}.
    """

    def setUp(self):
        """
        Build a database with data, a create a pool.
        """
        self.create_database()
        connection = self.connection = self.database.connect()
        self.drop_tables()
        self.create_tables()
        connection.execute("INSERT INTO foo VALUES (10, 'Title 30')")
        connection.execute("INSERT INTO bar VALUES (10, 10, 'Title 40')")
        connection.execute("INSERT INTO bar VALUES (11, 10, 'Title 50')")
        connection.commit()
        self.pool = StorePool(self.database, 2, 5)
        return self.pool.start()


    def tearDown(self):
        """
        Stop the pool.
        """
        def _drop(ign):
            self.drop_tables()
        return self.pool.stop().addCallback(_drop)


    def drop_tables(self):
        for table in ["foo", "bar"]:
            try:
                self.connection.execute("DROP TABLE %s" % table)
                self.connection.commit()
            except:
                self.connection.rollback()


    def test_already_started(self):
        """
        Check that the pool can't be restarted multiple times.
        """
        self.assertRaises(RuntimeError, self.pool.start)


    def test_get(self):
        """
        get should return different stores if available.
        """
        def cb_get1(store1):
            return self.pool.get().addCallback(cb_get2, store1)
        def cb_get2(store2, store1):
            self.assertNotIdentical(store1, store2)
            self.assertTrue(store1.started)
            self.assertTrue(store2.started)
        return self.pool.get().addCallback(cb_get1)


    def test_get_not_started(self):
        """
        If no store are available, the pool should create a store.
        """
        def cb(ign):
            self.assertEquals(self.pool._stores_created, 0)
            return self.pool.adjust_size(0, 5).addCallback(cb_add)
        def cb_add(ign):
            return self.pool.get().addCallback(cb_get)
        def cb_get(store):
            self.assertTrue(store.started)
        return self.pool.adjust_size(0, 0).addCallback(cb)


    def test_waiting_for_store(self):
        """
        Test waiting for a store availability.
        """
        def cb(ign):
            return self.pool.get().addCallback(cb_get1)
        def cb_get1(store1):
            self.assertTrue(store1.started)
            # Now we have a store, no store should be returned by the pool
            # until we put it back
            d1 = self.pool.get().addCallback(cb_get2, store1)
            d2 = self.pool.put(store1)
            return gatherResults([d1, d2])
        def cb_get2(store2, store1):
            self.assertIdentical(store1, store2)
        return self.pool.adjust_size(1, 1).addCallback(cb)


    def test_adjust_size_minmax(self):
        """
        Test sanity check on min/max - i.e. min <= max.
        """
        return self.assertFailure(self.pool.adjust_size(2, 1), ValueError)


    def test_adjust_size_nonnegative(self):
        """
        Test sanity check for nonnegative min.
        """
        return self.assertFailure(self.pool.adjust_size(-1), ValueError)


    @deferredGenerator
    def test_concurrent_data(self):
        """
        Test that different stores have different states: if the first store
        hasn't yet committed, the second one shouldn't get the new data.
        """
        foo = Foo()
        foo.title = u"Great title"
        foo.id = 11
        d = self.pool.get()
        wfd = waitForDeferred(d)
        yield wfd
        store1 = wfd.getResult()

        d = self.pool.get()
        wfd = waitForDeferred(d)
        yield wfd
        store2 = wfd.getResult()

        d = store1.add(foo)
        wfd = waitForDeferred(d)
        yield wfd
        wfd.getResult()

        d = store1.get(Foo, 11)
        wfd = waitForDeferred(d)
        yield wfd
        foo2 = wfd.getResult()

        # The object is already in the store cache
        self.assertIdentical(foo2, foo)

        d = store2.get(Foo, 11)
        wfd = waitForDeferred(d)
        yield wfd
        foo3 = wfd.getResult()

        # The object isn't in the db yet
        self.assertIdentical(foo3, None)

        # Let's rollback, because even select open a transaction
        d = store2.rollback()
        wfd = waitForDeferred(d)
        yield wfd
        wfd.getResult()

        # Let's commit
        d = store1.commit()
        wfd = waitForDeferred(d)
        yield wfd
        wfd.getResult()

        d = store2.get(Foo, 11)
        wfd = waitForDeferred(d)
        yield wfd
        foo4 = wfd.getResult()

        # The objects must be different
        self.assertNotIdentical(foo4, foo)
        # But the content must be the same
        self.assertEquals(foo4.title, u"Great title")


    def test_no_overflow(self):
        """
        Test that pool does not allocate more connections than store_max.
        """
        ds = []
        stores = set()

        def cb_get(store):
            stores.add(store)
            return self.pool.put(store)

        for i in range(10):
            ds.append(self.pool.get().addCallback(cb_get))

        def checkInstances(result):
            self.assertEquals(len(stores), 5)

        return gatherResults(ds).addCallback(checkInstances)


    def test_start_failure(self):
        """
        If a store failed to start, the number of allocated connections doesn't
        grow, so we're later able to start more stores.
        """
        ds = []
        stores = set()

        class DontStartStore(DeferredStore):
            def start(self):
                return fail(RuntimeError("oops"))

        calls = []

        def vicious_store_factory(database):
            if not calls:
                store = DontStartStore(database)
            else:
                store = DeferredStore(database)
            calls.append(None)
            return store

        self.pool.store_factory = vicious_store_factory

        def cb_get(store):
            stores.add(store)
            return self.pool.put(store)

        errors = []

        def save_errors(failure):
            errors.append(failure)

        for i in range(6):
            ds.append(
                self.pool.get().addCallback(cb_get).addErrback(save_errors))

        def checkInstances(result):
            self.assertEquals(len(stores), 5)
            self.assertEquals(len(errors), 1)
            errors[0].trap(RuntimeError)

        return gatherResults(ds).addCallback(checkInstances)


    def test_arguments(self):
        """
        Arguments are passed along to transacted method when store
        is available.
        """
        def tx(store, a, b=None):
            self.assertIsInstance(store, DeferredStore)
            self.assertEquals(1, a)
            self.assertEquals(2, b)
            return succeed("ok")
        return self.pool.transact(tx, 1, b=2)


    def test_commit(self):
        """
        Changes made inside a successful transaction are committed.
        """
        @deferredGenerator
        def check(result):
            d = self.pool.get()
            wfd = waitForDeferred(d)
            yield wfd
            store = wfd.getResult()
            d = store.execute("SELECT * FROM foo ORDER BY id", [])
            wfd = waitForDeferred(d)
            yield wfd
            dr = wfd.getResult()
            d = dr.get_all()
            wfd = waitForDeferred(d)
            yield wfd
            results = wfd.getResult()
            self.assertEquals(2, len(results))
            self.assertEquals(1, results[0][0])
            self.assertEquals("test", results[0][1])

        def tx(store):
            d = store.execute("INSERT INTO foo(id, title) "
                              "VALUES (1, 'test')", noresult=True)
            return d

        return self.pool.transact(tx).addCallback(check)


    def test_rollback(self):
        """
        Changes made inside a failed transaction are rolled back.
        """
        @deferredGenerator
        def check(reason):
            d = self.pool.get()
            wfd = waitForDeferred(d)
            yield wfd
            store = wfd.getResult()
            d = store.execute("SELECT * FROM foo", [])
            wfd = waitForDeferred(d)
            yield wfd
            dr = wfd.getResult()
            d = dr.get_all()
            wfd = waitForDeferred(d)
            yield wfd
            results = wfd.getResult()
            self.assertEquals(1, len(results))

        @deferredGenerator
        def tx(store):
            d = store.execute("INSERT INTO foo(id, title) "
                              "VALUES (1, 'test')", [])
            wfd = waitForDeferred(d)
            yield wfd
            wfd.getResult()
            d = store.execute("INSERT INTO foo(id, title) "
                              "VALUES (1, 'test')", [])
            wfd = waitForDeferred(d)
            yield wfd
            wfd.getResult()

        return self.pool.transact(tx).addBoth(check)


    def test_return_value(self):
        """
        Final return value should match return value from successful call to
        transacted function.
        """
        def check(result):
            self.assertEquals("completed", result)

        def cb(result):
            return "completed"

        def tx(store):
            return store.execute("INSERT INTO foo(id, title) "
                                 "VALUES (1, 'test')", []
                    ).addCallback(cb)

        return self.pool.transact(tx).addCallback(check)


    def test_poolsize_after_success(self):
        """
        After successful transaction, pool size should be same size as before.
        """
        size = len(self.pool._stores)

        def check(result):
            self.assertEquals(size, len(self.pool._stores))

        def tx(store):
            d = store.execute("SELECT * from foo", [])
            return d.addCallback(lambda result: result.get_all())

        return self.pool.transact(tx).addCallback(check)


    def test_poolsize_after_failure(self):
        """
        After failed transaction, pool size is restored to the initial value.
        """
        size = len(self.pool._stores)

        def check(reason):
            self.assertEquals(size, len(self.pool._stores))

        def tx(store):
            return store.execute("SELECT * from not_a_table", [])

        d = self.assertFailure(self.pool.transact(tx), OperationalError)
        return d.addCallback(check)


    def test_failure_propagation(self):
        """
        A custom exception is propagated by a C{transact} call.
        """
        class MyException(Exception):
            pass

        def tx(store):
            raise MyException("Bad things happened")

        return self.assertFailure(self.pool.transact(tx), MyException)


    def test_non_deferred_function(self):
        """
        C{transact} can handle functions that don't return a C{Deferred}.
        """
        def tx(store):
            return "foo"
        return self.pool.transact(tx).addCallback(self.assertEquals, "foo")


    def test_rollback_failure(self):
        """
        If C{rollback} fails, the store is put back into the pool.
        """

        def cb_get(store):
            store.rollback = lambda: fail(RuntimeError("oops"))
            d = self.assertFailure(self.pool.put(store), RuntimeError)
            return d.addCallback(get_all)

        def get_all(ignore):
            dl = []
            for i in range(5):
                dl.append(self.pool.get())
            return gatherResults(dl).addCallback(check)

        def check(stores):
            # The fact that we're here show that the test succeeds, because we
            # didn't hang waiting for a store
            self.assertEquals(len(stores), 5)

        return self.pool.get().addCallback(cb_get)
