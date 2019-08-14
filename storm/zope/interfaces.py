#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
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
from zope.interface import Interface

from storm.expr import Undef


class ZStormError(Exception):
    """
    Raised when situations such as duplicate store creation, unknown
    store name, etc., arise.
    """


class IZStorm(Interface):
    """A flag interface used to lookup the ZStorm utility."""


class IResultSet(Interface):
    """The interface for a L{ResultSet}.

    The rationale behind the exposed attributes is:

        1. Model code constructs a L{ResultSet} and returns a security proxied
           object to the view code.

        2. View code treats the L{ResultSet} as an immutable sequence/iterable
           and presents the data to the user.

    Therefore several attributes of L{ResultSet} are not included here:

        - Both C{set()} and C{remove()} can be used to modify the contained
          objects, which will bypass the security proxies on those objects.

        - C{get_select_expr()} will return a L{Select} object, which has no
          security declarations (and it isn't clear that any would be
          desirable).

        - C{find()}, C{group_by()} and C{having()} are really used to configure
          result sets, so are mostly intended for use on the model side.

        - There may be an argument for exposing C{difference()},
          C{intersection()} and C{union()} as a way for view code to combine
          multiple results, but it isn't clear how often it makes sense to do
          this on the view side rather than model side.
    """

    def copy():
        """
        Return a copy of this result set object, with the same configuration.
        """

    def config(distinct=None, offset=None, limit=None):
        """Configure the result set.

        @param distinct: Optionally, when true, only return distinct rows.
        @param offset: Optionally, the offset to start retrieving
            records from.
        @param limit: Optionally, the maximum number of rows to return.
        """

    def __iter__():
        """Iterate the result set."""

    def __getitem__(index):
        """
        Get the value at C{index} in the result set if C{index} is a
        single interger.  If C{index} is a slice a new C{ResultSet}
        will be returned.
        """

    def __contains__(item):
        """Check if C{item} is contained in the result set."""

    def any():
        """
        Get a random object from the result set or C{None} if the
        result set is empty.
        """

    def first():
        """Return the first item from an ordered result set.

        @raises UnorderedError: Raised if the result set isn't ordered.
        """

    def last():
        """Return the last item from an ordered result set.

        @raises UnorderedError: Raised if the result set isn't ordered.
        """

    def one():
        """
        Return one item from a result set containing at most one item
        or None if the result set is empty.

        @raises NotOneError: Raised if the result set contains more
            than one item.
        """

    def order_by(*args):
        """Order the result set based on expressions in C{args}."""

    def count(column=Undef, distinct=False):
        """Returns the number of rows in the result set.

        @param column: Optionally, the column to count.
        @param distinct: Optionally, when true, count only distinct rows.
        """

    def max(column):
        """Returns the maximum C{column} value in the result set."""

    def min():
        """Returns the minimum C{column} value in the result set."""

    def avg():
        """Returns the average of C{column} values in the result set."""

    def sum():
        """Returns the sum of C{column} values in the result set."""

    def values(*args):
        """Generator yields values for the columns specified in C{args}."""

    def cached():
        """Return matching objects from the cache for the current query."""

    def is_empty():
        """Return true if the result set contains no results."""


class ISQLObjectResultSet(Interface):

    def __getitem__(item):
       """List emulation."""

    def __getslice__(slice):
       """Slice support."""

    def __iter__():
       """List emulation."""

    def count():
       """Return the number of items in the result set."""

    def __nonzero__():
        """Return C{True} if this result set contains any results.

        @note: This method is provided for compatibility with SQL Object.  For
            new code, prefer L{is_empty}.  It's compatible with L{ResultSet}
            which doesn't have a C{__nonzero__} implementation.
        """

    def __contains__(item):
       """Support C{if FooObject in Foo.select(query)}."""

    def intersect(otherSelect, intersectAll=False, orderBy=None):
        """Return the intersection of this result and C{otherSelect}

        @param otherSelect: the other L{ISQLObjectResultSet}
        @param intersectAll: whether to use INTERSECT ALL behaviour
        @param orderBy: the order the result set should use.
        """

    def is_empty():
        """Return C{True} if this result set doesn't contain any results."""

    def prejoin(prejoins):
       """Return a new L{SelectResults} with the list of attributes prejoined.

       @param prejoins: The list of attribute names to prejoin.
       """
