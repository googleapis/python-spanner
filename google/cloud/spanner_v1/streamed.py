# Copyright 2016 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Wrapper for streaming results."""

from google.cloud import exceptions
from google.protobuf.struct_pb2 import ListValue
from google.protobuf.struct_pb2 import Value

from google.cloud.spanner_v1 import PartialResultSet
from google.cloud.spanner_v1 import ResultSetMetadata
from google.cloud.spanner_v1 import TypeCode
from google.cloud.spanner_v1._helpers import _parse_value_pb


class StreamedResultSet(object):
    """Process a sequence of partial result sets into a single set of row data.

    :type response_iterator:
    :param response_iterator:
        Iterator yielding
        :class:`~google.cloud.spanner_v1.types.PartialResultSet`
        instances.

    :type source: :class:`~google.cloud.spanner_v1.snapshot.Snapshot`
    :param source: Snapshot from which the result set was fetched.
    """

    def __init__(self, response_iterator, source=None):
        self._response_iterator = response_iterator
        self._rows = []  # Fully-processed rows
        self._metadata = None  # Until set from first PRS
        self._stats = None  # Until set from last PRS
        self._current_row = []  # Accumulated values for incomplete row
        self._pending_chunk = None  # Incomplete value
        self._source = source  # Source snapshot

    @property
    def fields(self):
        """Field descriptors for result set columns.

        :rtype: list of :class:`~google.cloud.spanner_v1.types.StructType.Field`
        :returns: list of fields describing column names / types.
        """
        return self._metadata.row_type.fields

    @property
    def metadata(self):
        """Result set metadata

        :rtype: :class:`~google.cloud.spanner_v1.types.ResultSetMetadata`
        :returns: structure describing the results
        """
        if self._metadata:
            return ResultSetMetadata.wrap(self._metadata)
        return None

    @property
    def stats(self):
        """Result set statistics

        :rtype:
           :class:`~google.cloud.spanner_v1.types.ResultSetStats`
        :returns: structure describing status about the response
        """
        return self._stats

    def _merge_chunk(self, value):
        """Merge pending chunk with next value.

        :type value: :class:`~google.protobuf.struct_pb2.Value`
        :param value: continuation of chunked value from previous
                      partial result set.

        :rtype: :class:`~google.protobuf.struct_pb2.Value`
        :returns: the merged value
        """
        current_column = len(self._current_row)
        field = self.fields[current_column]
        merged = _merge_by_type(self._pending_chunk, value, field.type_)
        self._pending_chunk = None
        return merged

    def _merge_values(self, values):
        """Merge values into rows.

        :type values: list of :class:`~google.protobuf.struct_pb2.Value`
        :param values: non-chunked values from partial result set.
        """
        field_types = [field.type_ for field in self.fields]
        width = len(field_types)
        index = len(self._current_row)
        for value in values:
            self._current_row.append(_parse_value_pb(value, field_types[index]))
            index += 1
            if index == width:
                self._rows.append(self._current_row)
                self._current_row = []
                index = 0

    def _consume_next(self):
        """Consume the next partial result set from the stream.

        Parse the result set into new/existing rows in :attr:`_rows`
        """
        response = next(self._response_iterator)
        response_pb = PartialResultSet.pb(response)

        if self._metadata is None:  # first response
            metadata = self._metadata = response_pb.metadata

            source = self._source
            if source is not None and source._transaction_id is None:
                source._transaction_id = metadata.transaction.id

        if response_pb.HasField("stats"):  # last response
            self._stats = response.stats

        values = list(response_pb.values)
        if self._pending_chunk is not None:
            values[0] = self._merge_chunk(values[0])

        if response_pb.chunked_value:
            self._pending_chunk = values.pop()

        self._merge_values(values)

    def __iter__(self):
        while True:
            iter_rows, self._rows[:] = self._rows[:], ()
            while iter_rows:
                yield iter_rows.pop(0)
            try:
                self._consume_next()
            except StopIteration:
                return

    def one(self):
        """Return exactly one result, or raise an exception.

        :raises: :exc:`NotFound`: If there are no results.
        :raises: :exc:`ValueError`: If there are multiple results.
        :raises: :exc:`RuntimeError`: If consumption has already occurred,
            in whole or in part.
        """
        answer = self.one_or_none()
        if answer is None:
            raise exceptions.NotFound("No rows matched the given query.")
        return answer

    def one_or_none(self):
        """Return exactly one result, or None if there are no results.

        :raises: :exc:`ValueError`: If there are multiple results.
        :raises: :exc:`RuntimeError`: If consumption has already occurred,
            in whole or in part.
        """
        # Sanity check: Has consumption of this query already started?
        # If it has, then this is an exception.
        if self._metadata is not None:
            raise RuntimeError(
                "Can not call `.one` or `.one_or_none` after "
                "stream consumption has already started."
            )

        # Consume the first result of the stream.
        # If there is no first result, then return None.
        iterator = iter(self)
        try:
            answer = next(iterator)
        except StopIteration:
            return None

        # Attempt to consume more. This should no-op; if we get additional
        # rows, then this is an error case.
        try:
            next(iterator)
            raise ValueError("Expected one result; got more.")
        except StopIteration:
            return answer


class Unmergeable(ValueError):
    """Unable to merge two values.

    :type lhs: :class:`~google.protobuf.struct_pb2.Value`
    :param lhs: pending value to be merged

    :type rhs: :class:`~google.protobuf.struct_pb2.Value`
    :param rhs: remaining value to be merged

    :type type_: :class:`~google.cloud.spanner_v1.types.Type`
    :param type_: field type of values being merged
    """

    def __init__(self, lhs, rhs, type_):
        message = "Cannot merge %s values: %s %s" % (
            TypeCode(type_.code),
            lhs,
            rhs,
        )
        super(Unmergeable, self).__init__(message)


def _unmergeable(lhs, rhs, type_):
    """Helper for '_merge_by_type'."""
    raise Unmergeable(lhs, rhs, type_)


def _merge_float64(lhs, rhs, type_):
    """Helper for '_merge_by_type'."""
    lhs_kind = lhs.WhichOneof("kind")
    if lhs_kind == "string_value":
        return Value(string_value=lhs.string_value + rhs.string_value)
    rhs_kind = rhs.WhichOneof("kind")
    array_continuation = (
        lhs_kind == "number_value"
        and rhs_kind == "string_value"
        and rhs.string_value == ""
    )
    if array_continuation:
        return lhs
    raise Unmergeable(lhs, rhs, type_)


def _merge_string(lhs, rhs, type_):
    """Helper for '_merge_by_type'."""
    return Value(string_value=lhs.string_value + rhs.string_value)


_UNMERGEABLE_TYPES = (TypeCode.BOOL,)


def _merge_array(lhs, rhs, type_):
    """Helper for '_merge_by_type'."""
    element_type = type_.array_element_type
    if element_type.code in _UNMERGEABLE_TYPES:
        # Individual values cannot be merged, just concatenate
        lhs.list_value.values.extend(rhs.list_value.values)
        return lhs
    lhs, rhs = list(lhs.list_value.values), list(rhs.list_value.values)

    # Sanity check: If either list is empty, short-circuit.
    # This is effectively a no-op.
    if not len(lhs) or not len(rhs):
        return Value(list_value=ListValue(values=(lhs + rhs)))

    first = rhs.pop(0)
    if first.HasField("null_value"):  # can't merge
        lhs.append(first)
    else:
        last = lhs.pop()
        if last.HasField("null_value"):
            lhs.append(last)
            lhs.append(first)
        else:
            try:
                merged = _merge_by_type(last, first, element_type)
            except Unmergeable:
                lhs.append(last)
                lhs.append(first)
            else:
                lhs.append(merged)
    return Value(list_value=ListValue(values=(lhs + rhs)))


def _merge_struct(lhs, rhs, type_):
    """Helper for '_merge_by_type'."""
    fields = type_.struct_type.fields
    lhs, rhs = list(lhs.list_value.values), list(rhs.list_value.values)

    # Sanity check: If either list is empty, short-circuit.
    # This is effectively a no-op.
    if not len(lhs) or not len(rhs):
        return Value(list_value=ListValue(values=(lhs + rhs)))

    candidate_type = fields[len(lhs) - 1].type_
    first = rhs.pop(0)
    if first.HasField("null_value") or candidate_type.code in _UNMERGEABLE_TYPES:
        lhs.append(first)
    else:
        last = lhs.pop()
        if last.HasField("null_value"):
            lhs.append(last)
            lhs.append(first)
        else:
            try:
                merged = _merge_by_type(last, first, candidate_type)
            except Unmergeable:
                lhs.append(last)
                lhs.append(first)
            else:
                lhs.append(merged)
    return Value(list_value=ListValue(values=lhs + rhs))


_MERGE_BY_TYPE = {
    TypeCode.ARRAY: _merge_array,
    TypeCode.BOOL: _unmergeable,
    TypeCode.BYTES: _merge_string,
    TypeCode.DATE: _merge_string,
    TypeCode.FLOAT64: _merge_float64,
    TypeCode.INT64: _merge_string,
    TypeCode.STRING: _merge_string,
    TypeCode.STRUCT: _merge_struct,
    TypeCode.TIMESTAMP: _merge_string,
    TypeCode.NUMERIC: _merge_string,
    TypeCode.JSON: _merge_string,
}


def _merge_by_type(lhs, rhs, type_):
    """Helper for '_merge_chunk'."""
    merger = _MERGE_BY_TYPE[type_.code]
    return merger(lhs, rhs, type_)
