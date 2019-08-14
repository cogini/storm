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
Asynchronous wrapper around storm.
"""

from storm.store import Store
from storm.references import Reference, ReferenceSet


try:
    from functools import partial
except ImportError:
    # For Python < 2.5
    class partial(object):
        def __init__(self, fn, *args, **kw):
            self.fn = fn
            self.args = args
            self.kw = kw

        def __call__(self, *args, **kw):
            if kw and self.kw:
                d = self.kw.copy()
                d.update(kw)
            else:
                d = kw or self.kw
            return self.fn(*(self.args + args), **d)



class DeferredResult(object):
    """
    Proxy for a storm result, running the blocking methods in a thread and
    returning C{Deferred}s.
    """

    def __init__(self, thread, result):
        """
        @param thread: the running thread of the store
        @type thread: C{StoreThread}

        @param result: the result instance to be wrapped.
        @type result: C{storm.database.Result}
        """
        self.result = result
        for methodName in ("get_one", "get_all"):
            method = partial(thread.defer_to_thread,
                             getattr(result, methodName))
            setattr(self, methodName, method)



class DeferredResultSet(object):
    """
    Wrapper for a L{storm.store.ResultSet}.
    """

    def __init__(self, thread, resultSet):
        """
        Create the results with given C{StoreThread} and the set to wrap.
        """
        self._thread = thread
        self._resultSet = resultSet
        for methodName in ("any", "one", "first", "last", "remove", "count",
                           "max", "min", "avg", "sum", "set", "is_empty"):
            method = partial(thread.defer_to_thread,
                             getattr(resultSet, methodName))
            setattr(self, methodName, method)
        for methodName in ("union", "difference", "intersection"):
            method = partial(self._set_expr,
                             getattr(resultSet, methodName))
            setattr(self, methodName, method)
        for methodName in ("order_by", "config", "group_by", "having"):
            setattr(self, methodName, getattr(self._resultSet, methodName))


    def all(self):
        """
        Specific method to emulate C{__iter__}.
        """
        return self._thread.defer_to_thread(list, self._resultSet)


    def values(self, *columns):
        """
        Wrapper around values that remove the iterator feature to return a list
        instead.
        """
        def _get_values():
            return list(self._resultSet.values(*columns))
        return self._thread.defer_to_thread(_get_values)


    def _set_expr(self, method, other, all=False):
        """
        Wrap a set expression with a C{DeferredResultSet}.
        """
        return DeferredResultSet(self._thread, method(other, all))



class DeferredReference(Reference):
    """
    A reference property but within a C{Deferred}.
    """

    def __get__(self, local, cls=None):
        """
        Wrapper around C{Reference.__get__}.
        """
        store = Store.of(local)
        if store is None:
            return None
        _thread = store._deferredStore.thread
        return _thread.defer_to_thread(Reference.__get__, self, local, cls)


    def __set__(self, local, remote):
        """
        Wrapper around C{Reference.__set__}.
        """
        raise RuntimeError("Can't set a DeferredReference")



class DeferredReferenceSet(ReferenceSet):
    """
    A C{ReferenceSet} but within a C{Deferred}.
    """

    def __get__(self, local, cls=None):
        """
        Wrapper around C{ReferenceSet.__get__}.
        """
        store = Store.of(local)
        if store is None:
            return None
        _thread = store._deferredStore.thread
        boundReference = ReferenceSet.__get__(self, local, cls)
        return DeferredBoundReference(_thread, boundReference)



class DeferredBoundReference(object):
    """
    Wrapper around C{BoundReferenceSet} and C{BoundIndirectReferenceSet}.
    """

    def __init__(self, thread, boundReference):
        """
        Create the reference with given C{StoreThread} and the reference to
        wrap.
        """
        self._thread = thread
        self._boundReference = boundReference
        for methodName in ("clear", "add", "remove", "any", "count", "one",
                           "first", "last"):
            method = partial(thread.defer_to_thread,
                             getattr(boundReference, methodName))
            setattr(self, methodName, method)
        for methodName in ("order_by", "find"):
            method = partial(self._defer_and_wrap_result,
                             getattr(boundReference, methodName))
            setattr(self, methodName, method)


    def all(self):
        """
        Specific method to emulate C{__iter__}.
        """
        return self._thread.defer_to_thread(list, self._boundReference)


    def values(self, *columns):
        """
        Emulate the values method.
        """
        def _get_values():
            return list(self._boundReference.values(*columns))
        return self._thread.defer_to_thread(_get_values)


    def _defer_and_wrap_result(self, method, *args, **kwargs):
        """
        Helper for methods returning another C{ResultSet}.
        """
        return self._thread.defer_to_thread(method, *args, **kwargs
            ).addCallback(lambda x: DeferredResultSet(self._thread, x))
