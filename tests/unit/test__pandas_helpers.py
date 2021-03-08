# Copyright 2021 Google LLC
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


import unittest


class TestPandasDataFrame(unittest.TestCase):
    def _getTargetClass(self):
        from google.cloud.spanner_v1.streamed import StreamedResultSet

        return StreamedResultSet

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    @staticmethod
    def _make_scalar_field(name, type_):
        from google.cloud.spanner_v1 import StructType
        from google.cloud.spanner_v1 import Type

        return StructType.Field(name=name, type_=Type(code=type_))

    @staticmethod
    def _make_value(value):
        from google.cloud.spanner_v1._helpers import _make_value_pb

        return _make_value_pb(value)

    @staticmethod
    def _make_result_set_metadata(fields=(), transaction_id=None):
        from google.cloud.spanner_v1 import ResultSetMetadata
        from google.cloud.spanner_v1 import StructType

        metadata = ResultSetMetadata(row_type=StructType(fields=[]))
        for field in fields:
            metadata.row_type.fields.append(field)
        if transaction_id is not None:
            metadata.transaction.id = transaction_id
        return metadata

    @staticmethod
    def _make_result_set_stats(query_plan=None, **kw):
        from google.cloud.spanner_v1 import ResultSetStats
        from google.protobuf.struct_pb2 import Struct
        from google.cloud.spanner_v1._helpers import _make_value_pb

        query_stats = Struct(
            fields={key: _make_value_pb(value) for key, value in kw.items()}
        )
        return ResultSetStats(query_plan=query_plan, query_stats=query_stats)

    def test_multiple_rows(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("Name", TypeCode.STRING),
            self._make_scalar_field("Age", TypeCode.INT64),
        ]
        metadata = streamed._metadata = self._make_result_set_metadata(FIELDS)
        stats = streamed._stats = self._make_result_set_stats()
        streamed._rows[:] = [["Alice", 1], ["Bob", 2], ["Adam", 3]]
        df_obj = streamed.to_dataframe()
        assert len(df_obj) == 3

    def test_single_rows(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("Name", TypeCode.STRING),
            self._make_scalar_field("Age", TypeCode.INT64),
        ]
        metadata = streamed._metadata = self._make_result_set_metadata(FIELDS)
        stats = streamed._stats = self._make_result_set_stats()
        streamed._rows[:] = [["Alice", 1]]
        df_obj = streamed.to_dataframe()
        assert len(df_obj) == 1

    def test_no_rows(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("Name", TypeCode.STRING),
            self._make_scalar_field("Age", TypeCode.INT64),
        ]
        metadata = streamed._metadata = self._make_result_set_metadata(FIELDS)
        stats = streamed._stats = self._make_result_set_stats()
        streamed._rows[:] = []
        df_obj = streamed.to_dataframe()
        assert len(df_obj) == 0


class _MockCancellableIterator(object):

    cancel_calls = 0

    def __init__(self, *values):
        self.iter_values = iter(values)

    def next(self):
        return next(self.iter_values)

    def __next__(self):  # pragma: NO COVER Py3k
        return self.next()
