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
Store wrapper and custom thread runner.
"""

from threading import Thread
from Queue import Queue

from storm.store import Store, AutoReload
from storm.info import get_obj_info
from storm.twisted.wrapper import partial, DeferredResult, DeferredResultSet

from twisted.internet.defer import Deferred, deferredGenerator, maybeDeferred
from twisted.internet.defer import waitForDeferred, succeed, fail
from twisted.python.failure import Failure



class AlreadyStopped(Exception):
    """
    Except raised when a store is stopped multiple time.
    """



class StoreThread(Thread):
    """
    A thread class that wraps methods calls and fires deferred in the reactor
    thread.
    """
    STOP = object()

    def __init__(self):
        """
        Initialize the thread, and create a L{Queue} to stack jobs.
        """
        Thread.__init__(self)
        self.setDaemon(True)
        self._queue = Queue()
        self._stop_deferred = None
        self.stopped = False


    def defer_to_thread(self, f, *args, **kwargs):
        """
        Run the given function in the thread, wrapping the result with a
        L{Deferred}.

        @return: a deferred whose result will be the result of the function.
        @rtype: C{Deferred}
        """
        if self.stopped:
            # to prevent having pending calls after the thread got stopped
            return fail(AlreadyStopped(f))
        d = Deferred()
        self._queue.put((d, f, args, kwargs))
        return d


    def run(self):
        """
        Main execution loop: retrieve jobs from the queue and run them.
        """
        from twisted.internet import reactor
        o = self._queue.get()
        while o is not self.STOP:
            d, f, args, kwargs = o
            try:
                result = f(*args, **kwargs)
            except:
                f = Failure()
                reactor.callFromThread(d.errback, f)
            else:
                reactor.callFromThread(d.callback, result)
            o = self._queue.get()
        reactor.callFromThread(self._stop_deferred.callback, None)


    def stop(self):
        """
        Stop the thread.
        """
        if self.stopped:
            return self._stop_deferred
        self._stop_deferred = Deferred()
        self._queue.put(self.STOP)
        self.stopped = True
        return self._stop_deferred



class DeferredStore(object):
    """
    A wrapper around L{Store} to have async operations.
    """
    store = None

    def __init__(self, database):
        """
        @param database: instance of database providing connection, used to
            instantiate the store later.
        @type database: L{storm.database.Database}
        """
        self.thread = StoreThread()
        self.database = database
        self.started = False


    def start(self):
        """
        Start the store.

        @return: a deferred that will fire once the store is started.
        """
        if not self.started:
            self.started = True
            self.thread.start()
            # Add a event trigger to be sure that the thread is stopped
            from twisted.internet import reactor
            reactor.addSystemEventTrigger(
                "before", "shutdown", self.stop)
            return self.thread.defer_to_thread(Store, self.database
                ).addCallback(self._got_store)
        else:
            raise RuntimeError("Already started")


    def _got_store(self, store):
        """
        Internal method called when the store is created, initializing most of
        the API methods.
        """
        self.store = store
        # Maybe not ?
        self.store._deferredStore = self
        for methodName in ("commit", "flush", "remove", "reload",
                           "rollback"):
            method = partial(self.thread.defer_to_thread,
                             getattr(self.store, methodName))
            setattr(self, methodName, method)

        self._do_resolve_lazy_value = self.store._resolve_lazy_value
        self.store._resolve_lazy_value = self._resolve_lazy_value


    def get(self, cls, key):
        def _get():
            obj = self.store.get(cls, key)
            if obj is not None:
                obj_info = get_obj_info(obj)
                self._do_resolve_lazy_value(obj_info, None, AutoReload)
            return obj
        return self.thread.defer_to_thread(_get)


    def add(self, obj):
        """
        Specific add method that doesn't return any result, to not make think
        that it's something usable.
        """
        def _add():
            self.store.add(obj)
        return self.thread.defer_to_thread(_add)


    def execute(self, *args, **kwargs):
        """
        Wrapper around C{execute} to have a C{DeferredResult} instead of the
        standard L{storm.database.Result} object.
        """
        if self.store is None:
            raise RuntimeError("Store not started")
        return self.thread.defer_to_thread(
            self.store.execute, *args, **kwargs
            ).addCallback(self._cb_execute)


    def _cb_execute(self, result):
        """
        Wrap the result with a C{DeferredResult}.
        """
        if result is not None:
            return DeferredResult(self.thread, result)


    def find(self, *args, **kwargs):
        """
        Wrapper around C{find}.
        """
        if self.store is None:
            raise RuntimeError("Store not started")
        return self.thread.defer_to_thread(
            self.store.find, *args, **kwargs
            ).addCallback(self._cb_find)


    def _cb_find(self, resultSet):
        """
        Wrap the result set with a C{DeferredResultSet}.
        """
        return DeferredResultSet(self.thread, resultSet)


    def stop(self):
        """
        Stop the store.
        """
        if self.thread.stopped:
            return succeed(None)
        def close():
            self.store.rollback()
            self.store.close()
        return self.thread.defer_to_thread(close
            ).addCallback(lambda ign: self.thread.stop())


    def _resolve_lazy_value(self, *args):
        raise RuntimeError(
            "Resolving lazy values with the Twisted wrapper is not possible "
            "right now! Please refetch your object using "
            "store.get/store.find")


    @staticmethod
    def of(obj):
        """
        Get the DeferredStore object is associated with

        If the given object has not been associated with a DeferredStore,
        return None.
        """
        store = Store.of(obj)
        if not store:
            return
        return getattr(store, '_deferredStore', None)



class StorePool(object):
    """
    A pool of started stores, maintaining persistent connections.
    """
    started = False
    store_factory = DeferredStore

    def __init__(self, database, min_stores=0, max_stores=10):
        """
        @param database: instance of database providing connection, used to
            instantiate the store later.
        @type database: L{storm.database.Database}

        @param min_stores: initial number of stores.
        @type min_stores: C{int}

        @param max_stores: maximum number of stores.
        @type max_stores: C{int}
        """
        self.database = database
        self.min_stores = min_stores
        self.max_stores = max_stores
        self._stores = []
        self._stores_created = 0
        self._pending_get = []
        self._store_refs = []


    def start(self):
        """
        Start the pool.
        """
        if self.started:
            raise RuntimeError("Already started")
        self.started = True
        return self.adjust_size()


    def stop(self):
        """
        Stop the pool: this is not a total stop, it just try to kill the
        current available stores.
        """
        return self.adjust_size(0, 0, self._store_refs)


    def _start_store(self):
        """
        Create a new store.
        """
        store = self.store_factory(self.database)
        # Increment here, so that other simultaneous calls don't make the
        # number of connections pass the maximum
        self._stores_created += 1
        return store.start(
            ).addCallback(self._cb_start_store, store
            ).addErrback(self._eb_start_store)


    def _cb_start_store(self, ign, store):
        """
        Add the created store to the list of available stores.
        """
        self._stores.append(store)
        self._store_refs.append(store)


    def _eb_start_store(self, failure):
        """
        Reduce the amount of created stores, and let the failure propagate.
        """
        self._stores_created -= 1
        return failure


    def _stop_store(self, stores=None):
        """
        Stop a store and remove it from the available stores.
        """
        if stores is None:
            stores = self._stores
        self._stores_created -= 1
        store = stores.pop()
        return store.stop()


    @deferredGenerator
    def adjust_size(self, min_stores=None, max_stores=None, stores=None):
        """
        Change the number of available stores, shrinking or raising as
        necessary.
        """
        if min_stores is None:
            min_stores = self.min_stores
        if max_stores is None:
            max_stores = self.max_stores
        if stores is None:
            stores = self._stores

        if min_stores < 0:
            raise ValueError('minimum is negative')
        if min_stores > max_stores:
            raise ValueError('minimum is greater than maximum')

        self.min_stores = min_stores
        self.max_stores = max_stores
        if not self.started:
            return

        # Kill of some stores if we have too many.
        while self._stores_created > self.max_stores and stores:
            wfd = waitForDeferred(self._stop_store(stores))
            yield wfd
            wfd.getResult()
        # Start some stores if we have too few.
        while self._stores_created < self.min_stores:
            wfd = waitForDeferred(self._start_store())
            yield wfd
            wfd.getResult()


    def get(self):
        """
        Return a started store from the pool, or start a new one if necessary.
        A store retrieve by this way should be put back using the put
        method, or it won't be used anymore.
        """
        if not self.started:
            raise RuntimeError("Not started")
        if self._stores:
            store = self._stores.pop()
            return succeed(store)
        elif self._stores_created < self.max_stores:
            return self._start_store().addCallback(self._cb_get)
        else:
            # Maybe all stores are consumed?
            return self.adjust_size().addCallback(self._cb_get)


    def _cb_get(self, ign):
        """
        If the previous operation added a store, return it, or return a pending
        C{Deferred}.
        """
        if self._stores:
            store = self._stores.pop()
            return store
        else:
            # All stores are in used, wait
            d = Deferred()
            self._pending_get.append(d)
            return d


    def put(self, store):
        """
        Make a store available again.

        This should be done explicitely to have the store back in the pool.
        The good way to use the pool is this:

        >>> d1 = pool.get()

        >>> # d1 callback with a store
        >>> d2 = store.add(foo)
        >>> d2.addCallback(doSomething).addErrback(manageErrors)
        >>> d2.addBoth(lambda x: pool.put(store))
        """
        return store.rollback().addBoth(self._cb_put, store)


    def _cb_put(self, passthrough, store):
        """
        Once the rollback has finished, the store is really available.
        """
        if self._pending_get:
            # People are waiting, fire with the store
            d = self._pending_get.pop(0)
            d.callback(store)
        else:
            self._stores.append(store)
        return passthrough


    def transact(self, f, *args, **kwargs):
        """
        Call function C{f} with a L{Store} instance and arguments C{args} and
        C{kwargs} in transaction bound to the acquired store.  If transaction
        succeeds, store will be commited.  Store is returned to this pool after
        call to C{f} completes.

        Note that the function C{f} must return an instance of L{Deferred}.

        @param f: function to call in transaction
        @param args: positional arguments to function C{f}
        @param kwargs: keyword arguments to function C{f}
        """
        return self.get(
            ).addCallback(self._cb_transact_start, f, args, kwargs)


    def _cb_transact_start(self, store, f, args, kwargs):
        """
        Call transacted function with acquired store.
        """
        result = maybeDeferred(f, store, *args, **kwargs)
        result.addCallback(self._cb_transact_success, store)
        result.addBoth(self._cb_transact_stop, store)
        return result


    def _cb_transact_success(self, result, store):
        """
        Commit and pass through function result.
        """
        return store.commit().addCallback(lambda ignore: result)


    def _cb_transact_stop(self, result, store):
        """
        Return the store back to the pool and pass through the result again.
        """
        return self.put(store).addCallback(lambda ignore: result)
