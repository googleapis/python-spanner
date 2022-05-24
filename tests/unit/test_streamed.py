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


import unittest

import mock


class TestStreamedResultSet(unittest.TestCase):
    def _getTargetClass(self):
        from google.cloud.spanner_v1.streamed import StreamedResultSet

        return StreamedResultSet

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_ctor_defaults(self):
        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        self.assertIs(streamed._response_iterator, iterator)
        self.assertIsNone(streamed._source)
        self.assertEqual(list(streamed), [])
        self.assertIsNone(streamed.metadata)
        self.assertIsNone(streamed.stats)

    def test_ctor_w_source(self):
        iterator = _MockCancellableIterator()
        source = object()
        streamed = self._make_one(iterator, source=source)
        self.assertIs(streamed._response_iterator, iterator)
        self.assertIs(streamed._source, source)
        self.assertEqual(list(streamed), [])
        self.assertIsNone(streamed.metadata)
        self.assertIsNone(streamed.stats)

    def test_fields_unset(self):
        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        with self.assertRaises(AttributeError):
            streamed.fields

    @staticmethod
    def _make_scalar_field(name, type_):
        from google.cloud.spanner_v1 import StructType
        from google.cloud.spanner_v1 import Type

        return StructType.Field(name=name, type_=Type(code=type_))

    @staticmethod
    def _make_array_field(name, element_type_code=None, element_type=None):
        from google.cloud.spanner_v1 import StructType
        from google.cloud.spanner_v1 import Type
        from google.cloud.spanner_v1 import TypeCode

        if element_type is None:
            element_type = Type(code=element_type_code)
        array_type = Type(code=TypeCode.ARRAY, array_element_type=element_type)
        return StructType.Field(name=name, type_=array_type)

    @staticmethod
    def _make_struct_type(struct_type_fields):
        from google.cloud.spanner_v1 import StructType
        from google.cloud.spanner_v1 import Type
        from google.cloud.spanner_v1 import TypeCode

        fields = [
            StructType.Field(name=key, type_=Type(code=value))
            for key, value in struct_type_fields
        ]
        struct_type = StructType(fields=fields)
        return Type(code=TypeCode.STRUCT, struct_type=struct_type)

    @staticmethod
    def _make_value(value):
        from google.cloud.spanner_v1._helpers import _make_value_pb

        return _make_value_pb(value)

    @staticmethod
    def _make_list_value(values=(), value_pbs=None):
        from google.protobuf.struct_pb2 import ListValue
        from google.protobuf.struct_pb2 import Value
        from google.cloud.spanner_v1._helpers import _make_list_value_pb

        if value_pbs is not None:
            return Value(list_value=ListValue(values=value_pbs))
        return Value(list_value=_make_list_value_pb(values))

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

    @staticmethod
    def _make_partial_result_set(
        values, metadata=None, stats=None, chunked_value=False
    ):
        from google.cloud.spanner_v1 import PartialResultSet

        results = PartialResultSet(
            metadata=metadata, stats=stats, chunked_value=chunked_value
        )
        for v in values:
            results.values.append(v)
        return results

    def test_properties_set(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
        ]
        metadata = streamed._metadata = self._make_result_set_metadata(FIELDS)
        stats = streamed._stats = self._make_result_set_stats()
        self.assertEqual(list(streamed.fields), FIELDS)
        self.assertIs(streamed.metadata._pb, metadata)
        self.assertIs(streamed.stats, stats)

    def test__merge_chunk_bool(self):
        from google.cloud.spanner_v1.streamed import Unmergeable
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_scalar_field("registered_voter", TypeCode.BOOL)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = True
        chunk = False

        with self.assertRaises(Unmergeable):
            streamed._merge_chunk(chunk)

    def test__merge_chunk_numeric(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_scalar_field("total", TypeCode.NUMERIC)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_value("1234.")
        chunk = self._make_value("5678")

        merged = streamed._merge_chunk(chunk)
        self.assertEqual(merged.string_value, "1234.5678")

    def test__merge_chunk_int64(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_scalar_field("age", TypeCode.INT64)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_value(42)
        chunk = self._make_value(13)

        merged = streamed._merge_chunk(chunk)
        self.assertEqual(merged.string_value, "4213")
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_float64_nan_string(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_scalar_field("weight", TypeCode.FLOAT64)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_value("Na")
        chunk = self._make_value("N")

        merged = streamed._merge_chunk(chunk)
        self.assertEqual(merged.string_value, "NaN")

    def test__merge_chunk_float64_w_empty(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_scalar_field("weight", TypeCode.FLOAT64)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_value(3.14159)
        chunk = self._make_value("")

        merged = streamed._merge_chunk(chunk)
        self.assertEqual(merged.number_value, 3.14159)

    def test__merge_chunk_float64_w_float64(self):
        from google.cloud.spanner_v1.streamed import Unmergeable
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_scalar_field("weight", TypeCode.FLOAT64)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_value(3.14159)
        chunk = self._make_value(2.71828)

        with self.assertRaises(Unmergeable):
            streamed._merge_chunk(chunk)

    def test__merge_chunk_string(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_scalar_field("name", TypeCode.STRING)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_value("phred")
        chunk = self._make_value("wylma")

        merged = streamed._merge_chunk(chunk)

        self.assertEqual(merged.string_value, "phredwylma")
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_string_w_bytes(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_scalar_field("image", TypeCode.BYTES)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_value(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA"
            "6fptVAAAACXBIWXMAAAsTAAALEwEAmpwYAAAA\n"
        )
        chunk = self._make_value(
            "B3RJTUUH4QQGFwsBTL3HMwAAABJpVFh0Q29tbWVudAAAAAAAU0FNUExF"
            "MG3E+AAAAApJREFUCNdj\nYAAAAAIAAeIhvDMAAAAASUVORK5CYII=\n"
        )

        merged = streamed._merge_chunk(chunk)

        self.assertEqual(
            merged.string_value,
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACXBIWXMAAAsTAAAL"
            "EwEAmpwYAAAA\nB3RJTUUH4QQGFwsBTL3HMwAAABJpVFh0Q29tbWVudAAAAAAAU0"
            "FNUExFMG3E+AAAAApJREFUCNdj\nYAAAAAIAAeIhvDMAAAAASUVORK5CYII=\n",
        )
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_bool(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_array_field("name", element_type_code=TypeCode.BOOL)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value([True, True])
        chunk = self._make_list_value([False, False, False])

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value([True, True, False, False, False])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_int(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_array_field("name", element_type_code=TypeCode.INT64)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value([0, 1, 2])
        chunk = self._make_list_value([3, 4, 5])

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value([0, 1, 23, 4, 5])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_float(self):
        from google.cloud.spanner_v1 import TypeCode
        import math

        PI = math.pi
        EULER = math.e
        SQRT_2 = math.sqrt(2.0)
        LOG_10 = math.log(10)
        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_array_field("name", element_type_code=TypeCode.FLOAT64)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value([PI, SQRT_2])
        chunk = self._make_list_value(["", EULER, LOG_10])

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value([PI, SQRT_2, EULER, LOG_10])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_string_with_empty(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_array_field("name", element_type_code=TypeCode.STRING)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value(["A", "B", "C"])
        chunk = self._make_list_value([])

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value(["A", "B", "C"])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_string(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_array_field("name", element_type_code=TypeCode.STRING)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value(["A", "B", "C"])
        chunk = self._make_list_value(["D", "E"])

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value(["A", "B", "CD", "E"])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_string_with_null(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_array_field("name", element_type_code=TypeCode.STRING)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value(["A", "B", "C"])
        chunk = self._make_list_value([None, "D", "E"])

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value(["A", "B", "C", None, "D", "E"])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_string_with_null_pending(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [self._make_array_field("name", element_type_code=TypeCode.STRING)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value(["A", "B", "C", None])
        chunk = self._make_list_value(["D", "E"])
        merged = streamed._merge_chunk(chunk)
        expected = self._make_list_value(["A", "B", "C", None, "D", "E"])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_array_of_int(self):
        from google.cloud.spanner_v1 import StructType
        from google.cloud.spanner_v1 import Type
        from google.cloud.spanner_v1 import TypeCode

        subarray_type = Type(
            code=TypeCode.ARRAY, array_element_type=Type(code=TypeCode.INT64)
        )
        array_type = Type(code=TypeCode.ARRAY, array_element_type=subarray_type)
        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [StructType.Field(name="loloi", type_=array_type)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value(
            value_pbs=[self._make_list_value([0, 1]), self._make_list_value([2])]
        )
        chunk = self._make_list_value(
            value_pbs=[self._make_list_value([3]), self._make_list_value([4, 5])]
        )

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value(
            value_pbs=[
                self._make_list_value([0, 1]),
                self._make_list_value([23]),
                self._make_list_value([4, 5]),
            ]
        )
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_array_of_string(self):
        from google.cloud.spanner_v1 import StructType
        from google.cloud.spanner_v1 import Type
        from google.cloud.spanner_v1 import TypeCode

        subarray_type = Type(
            code=TypeCode.ARRAY, array_element_type=Type(code=TypeCode.STRING)
        )
        array_type = Type(code=TypeCode.ARRAY, array_element_type=subarray_type)
        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [StructType.Field(name="lolos", type_=array_type)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_list_value(
            value_pbs=[
                self._make_list_value(["A", "B"]),
                self._make_list_value(["C"]),
            ]
        )
        chunk = self._make_list_value(
            value_pbs=[
                self._make_list_value(["D"]),
                self._make_list_value(["E", "F"]),
            ]
        )

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value(
            value_pbs=[
                self._make_list_value(["A", "B"]),
                self._make_list_value(["CD"]),
                self._make_list_value(["E", "F"]),
            ]
        )
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_struct(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        struct_type = self._make_struct_type(
            [("name", TypeCode.STRING), ("age", TypeCode.INT64)]
        )
        FIELDS = [self._make_array_field("test", element_type=struct_type)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        partial = self._make_list_value(["Phred "])
        streamed._pending_chunk = self._make_list_value(value_pbs=[partial])
        rest = self._make_list_value(["Phlyntstone", 31])
        chunk = self._make_list_value(value_pbs=[rest])

        merged = streamed._merge_chunk(chunk)

        struct = self._make_list_value(["Phred Phlyntstone", 31])
        expected = self._make_list_value(value_pbs=[struct])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_struct_with_empty(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        struct_type = self._make_struct_type(
            [("name", TypeCode.STRING), ("age", TypeCode.INT64)]
        )
        FIELDS = [self._make_array_field("test", element_type=struct_type)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        partial = self._make_list_value(["Phred "])
        streamed._pending_chunk = self._make_list_value(value_pbs=[partial])
        rest = self._make_list_value([])
        chunk = self._make_list_value(value_pbs=[rest])

        merged = streamed._merge_chunk(chunk)

        expected = self._make_list_value(value_pbs=[partial])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_struct_unmergeable(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        struct_type = self._make_struct_type(
            [
                ("name", TypeCode.STRING),
                ("registered", TypeCode.BOOL),
                ("voted", TypeCode.BOOL),
            ]
        )
        FIELDS = [self._make_array_field("test", element_type=struct_type)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        partial = self._make_list_value(["Phred Phlyntstone", True])
        streamed._pending_chunk = self._make_list_value(value_pbs=[partial])
        rest = self._make_list_value([True])
        chunk = self._make_list_value(value_pbs=[rest])

        merged = streamed._merge_chunk(chunk)

        struct = self._make_list_value(["Phred Phlyntstone", True, True])
        expected = self._make_list_value(value_pbs=[struct])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test__merge_chunk_array_of_struct_unmergeable_split(self):
        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        struct_type = self._make_struct_type(
            [("name", "STRING"), ("height", "FLOAT64"), ("eye_color", "STRING")]
        )
        FIELDS = [self._make_array_field("test", element_type=struct_type)]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        partial = self._make_list_value(["Phred Phlyntstone", 1.65])
        streamed._pending_chunk = self._make_list_value(value_pbs=[partial])
        rest = self._make_list_value(["brown"])
        chunk = self._make_list_value(value_pbs=[rest])

        merged = streamed._merge_chunk(chunk)

        struct = self._make_list_value(["Phred Phlyntstone", 1.65, "brown"])
        expected = self._make_list_value(value_pbs=[struct])
        self.assertEqual(merged, expected)
        self.assertIsNone(streamed._pending_chunk)

    def test_merge_values_empty_and_empty(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._current_row = []
        streamed._merge_values([])
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, [])

    def test_merge_values_empty_and_partial(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        BARE = ["Phred Phlyntstone", 42]
        VALUES = [self._make_value(bare) for bare in BARE]
        streamed._current_row = []
        streamed._merge_values(VALUES)
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, BARE)

    def test_merge_values_empty_and_filled(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        BARE = ["Phred Phlyntstone", 42, True]
        VALUES = [self._make_value(bare) for bare in BARE]
        streamed._current_row = []
        streamed._merge_values(VALUES)
        self.assertEqual(list(streamed), [BARE])
        self.assertEqual(streamed._current_row, [])

    def test_merge_values_empty_and_filled_plus(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        BARE = [
            "Phred Phlyntstone",
            42,
            True,
            "Bharney Rhubble",
            39,
            True,
            "Wylma Phlyntstone",
        ]
        VALUES = [self._make_value(bare) for bare in BARE]
        streamed._current_row = []
        streamed._merge_values(VALUES)
        self.assertEqual(list(streamed), [BARE[0:3], BARE[3:6]])
        self.assertEqual(streamed._current_row, BARE[6:])

    def test_merge_values_partial_and_empty(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        BEFORE = ["Phred Phlyntstone"]
        streamed._current_row[:] = BEFORE
        streamed._merge_values([])
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, BEFORE)

    def test_merge_values_partial_and_partial(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        BEFORE = ["Phred Phlyntstone"]
        streamed._current_row[:] = BEFORE
        MERGED = [42]
        TO_MERGE = [self._make_value(item) for item in MERGED]
        streamed._merge_values(TO_MERGE)
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, BEFORE + MERGED)

    def test_merge_values_partial_and_filled(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        BEFORE = ["Phred Phlyntstone"]
        streamed._current_row[:] = BEFORE
        MERGED = [42, True]
        TO_MERGE = [self._make_value(item) for item in MERGED]
        streamed._merge_values(TO_MERGE)
        self.assertEqual(list(streamed), [BEFORE + MERGED])
        self.assertEqual(streamed._current_row, [])

    def test_merge_values_partial_and_filled_plus(self):
        from google.cloud.spanner_v1 import TypeCode

        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        BEFORE = [self._make_value("Phred Phlyntstone")]
        streamed._current_row[:] = BEFORE
        MERGED = [42, True, "Bharney Rhubble", 39, True, "Wylma Phlyntstone"]
        TO_MERGE = [self._make_value(item) for item in MERGED]
        VALUES = BEFORE + MERGED
        streamed._merge_values(TO_MERGE)
        self.assertEqual(list(streamed), [VALUES[0:3], VALUES[3:6]])
        self.assertEqual(streamed._current_row, VALUES[6:])

    def test_one_or_none_no_value(self):
        streamed = self._make_one(_MockCancellableIterator())
        with mock.patch.object(streamed, "_consume_next") as consume_next:
            consume_next.side_effect = StopIteration
            self.assertIsNone(streamed.one_or_none())

    def test_one_or_none_single_value(self):
        streamed = self._make_one(_MockCancellableIterator())
        streamed._rows = ["foo"]
        with mock.patch.object(streamed, "_consume_next") as consume_next:
            consume_next.side_effect = StopIteration
            self.assertEqual(streamed.one_or_none(), "foo")

    def test_one_or_none_multiple_values(self):
        streamed = self._make_one(_MockCancellableIterator())
        streamed._rows = ["foo", "bar"]
        with self.assertRaises(ValueError):
            streamed.one_or_none()

    def test_one_or_none_consumed_stream(self):
        streamed = self._make_one(_MockCancellableIterator())
        streamed._metadata = object()
        with self.assertRaises(RuntimeError):
            streamed.one_or_none()

    def test_one_single_value(self):
        streamed = self._make_one(_MockCancellableIterator())
        streamed._rows = ["foo"]
        with mock.patch.object(streamed, "_consume_next") as consume_next:
            consume_next.side_effect = StopIteration
            self.assertEqual(streamed.one(), "foo")

    def test_one_no_value(self):
        from google.cloud import exceptions

        iterator = _MockCancellableIterator(["foo"])
        streamed = self._make_one(iterator)
        with mock.patch.object(streamed, "_consume_next") as consume_next:
            consume_next.side_effect = StopIteration
            with self.assertRaises(exceptions.NotFound):
                streamed.one()

    def test_consume_next_empty(self):
        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        with self.assertRaises(StopIteration):
            streamed._consume_next()

    def test_consume_next_first_set_partial(self):
        from google.cloud.spanner_v1 import TypeCode

        TXN_ID = b"DEADBEEF"
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        metadata = self._make_result_set_metadata(FIELDS, transaction_id=TXN_ID)
        BARE = ["Phred Phlyntstone", 42]
        VALUES = [self._make_value(bare) for bare in BARE]
        result_set = self._make_partial_result_set(VALUES, metadata=metadata)
        iterator = _MockCancellableIterator(result_set)
        source = mock.Mock(_transaction_id=None, spec=["_transaction_id"])
        streamed = self._make_one(iterator, source=source)
        streamed._consume_next()
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, BARE)
        self.assertEqual(streamed.metadata, metadata)
        self.assertEqual(source._transaction_id, TXN_ID)

    def test_consume_next_first_set_partial_existing_txn_id(self):
        from google.cloud.spanner_v1 import TypeCode

        TXN_ID = b"DEADBEEF"
        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        metadata = self._make_result_set_metadata(FIELDS, transaction_id=b"")
        BARE = ["Phred Phlyntstone", 42]
        VALUES = [self._make_value(bare) for bare in BARE]
        result_set = self._make_partial_result_set(VALUES, metadata=metadata)
        iterator = _MockCancellableIterator(result_set)
        source = mock.Mock(_transaction_id=TXN_ID, spec=["_transaction_id"])
        streamed = self._make_one(iterator, source=source)
        streamed._consume_next()
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, BARE)
        self.assertEqual(streamed.metadata, metadata)
        self.assertEqual(source._transaction_id, TXN_ID)

    def test_consume_next_w_partial_result(self):
        from google.cloud.spanner_v1 import TypeCode

        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        VALUES = [self._make_value("Phred ")]
        result_set = self._make_partial_result_set(VALUES, chunked_value=True)
        iterator = _MockCancellableIterator(result_set)
        streamed = self._make_one(iterator)
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._consume_next()
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, [])
        self.assertEqual(streamed._pending_chunk, VALUES[0])

    def test_consume_next_w_pending_chunk(self):
        from google.cloud.spanner_v1 import TypeCode

        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        BARE = [
            "Phlyntstone",
            42,
            True,
            "Bharney Rhubble",
            39,
            True,
            "Wylma Phlyntstone",
        ]
        VALUES = [self._make_value(bare) for bare in BARE]
        result_set = self._make_partial_result_set(VALUES)
        iterator = _MockCancellableIterator(result_set)
        streamed = self._make_one(iterator)
        streamed._metadata = self._make_result_set_metadata(FIELDS)
        streamed._pending_chunk = self._make_value("Phred ")
        streamed._consume_next()
        self.assertEqual(
            list(streamed),
            [["Phred Phlyntstone", BARE[1], BARE[2]], [BARE[3], BARE[4], BARE[5]]],
        )
        self.assertEqual(streamed._current_row, [BARE[6]])
        self.assertIsNone(streamed._pending_chunk)

    def test_consume_next_last_set(self):
        from google.cloud.spanner_v1 import TypeCode

        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        metadata = self._make_result_set_metadata(FIELDS)
        stats = self._make_result_set_stats(
            rows_returned="1", elapsed_time="1.23 secs", cpu_time="0.98 secs"
        )
        BARE = ["Phred Phlyntstone", 42, True]
        VALUES = [self._make_value(bare) for bare in BARE]
        result_set = self._make_partial_result_set(VALUES, stats=stats)
        iterator = _MockCancellableIterator(result_set)
        streamed = self._make_one(iterator)
        streamed._metadata = metadata
        streamed._consume_next()
        self.assertEqual(list(streamed), [BARE])
        self.assertEqual(streamed._current_row, [])
        self.assertEqual(streamed._stats, stats)

    def test___iter___empty(self):
        iterator = _MockCancellableIterator()
        streamed = self._make_one(iterator)
        found = list(streamed)
        self.assertEqual(found, [])

    def test___iter___one_result_set_partial(self):
        from google.cloud.spanner_v1 import TypeCode
        from google.protobuf.struct_pb2 import Value

        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        metadata = self._make_result_set_metadata(FIELDS)
        BARE = ["Phred Phlyntstone", 42]
        VALUES = [self._make_value(bare) for bare in BARE]
        for val in VALUES:
            self.assertIsInstance(val, Value)
        result_set = self._make_partial_result_set(VALUES, metadata=metadata)
        iterator = _MockCancellableIterator(result_set)
        streamed = self._make_one(iterator)
        found = list(streamed)
        self.assertEqual(found, [])
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, BARE)
        self.assertEqual(streamed.metadata, metadata)

    def test___iter___multiple_result_sets_filled(self):
        from google.cloud.spanner_v1 import TypeCode

        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        metadata = self._make_result_set_metadata(FIELDS)
        BARE = [
            "Phred Phlyntstone",
            42,
            True,
            "Bharney Rhubble",
            39,
            True,
            "Wylma Phlyntstone",
            41,
            True,
        ]
        VALUES = [self._make_value(bare) for bare in BARE]
        result_set1 = self._make_partial_result_set(VALUES[:4], metadata=metadata)
        result_set2 = self._make_partial_result_set(VALUES[4:])
        iterator = _MockCancellableIterator(result_set1, result_set2)
        streamed = self._make_one(iterator)
        found = list(streamed)
        self.assertEqual(
            found,
            [
                [BARE[0], BARE[1], BARE[2]],
                [BARE[3], BARE[4], BARE[5]],
                [BARE[6], BARE[7], BARE[8]],
            ],
        )
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, [])
        self.assertIsNone(streamed._pending_chunk)

    def test___iter___w_existing_rows_read(self):
        from google.cloud.spanner_v1 import TypeCode

        FIELDS = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
            self._make_scalar_field("married", TypeCode.BOOL),
        ]
        metadata = self._make_result_set_metadata(FIELDS)
        ALREADY = [["Pebbylz Phlyntstone", 4, False], ["Dino Rhubble", 4, False]]
        BARE = [
            "Phred Phlyntstone",
            42,
            True,
            "Bharney Rhubble",
            39,
            True,
            "Wylma Phlyntstone",
            41,
            True,
        ]
        VALUES = [self._make_value(bare) for bare in BARE]
        result_set1 = self._make_partial_result_set(VALUES[:4], metadata=metadata)
        result_set2 = self._make_partial_result_set(VALUES[4:])
        iterator = _MockCancellableIterator(result_set1, result_set2)
        streamed = self._make_one(iterator)
        streamed._rows[:] = ALREADY
        found = list(streamed)
        self.assertEqual(
            found,
            ALREADY
            + [
                [BARE[0], BARE[1], BARE[2]],
                [BARE[3], BARE[4], BARE[5]],
                [BARE[6], BARE[7], BARE[8]],
            ],
        )
        self.assertEqual(list(streamed), [])
        self.assertEqual(streamed._current_row, [])
        self.assertIsNone(streamed._pending_chunk)


class _MockCancellableIterator(object):

    cancel_calls = 0

    def __init__(self, *values):
        self.iter_values = iter(values)

    def next(self):
        return next(self.iter_values)

    def __next__(self):  # pragma: NO COVER Py3k
        return self.next()


class TestStreamedResultSet_JSON_acceptance_tests(unittest.TestCase):

    _json_tests = None

    def _getTargetClass(self):
        from google.cloud.spanner_v1.streamed import StreamedResultSet

        return StreamedResultSet

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def _load_json_test(self, test_name):
        import os

        if self.__class__._json_tests is None:
            dirname = os.path.dirname(__file__)
            filename = os.path.join(dirname, "streaming-read-acceptance-test.json")
            raw = _parse_streaming_read_acceptance_tests(filename)
            tests = self.__class__._json_tests = {}
            for (name, partial_result_sets, results) in raw:
                tests[name] = partial_result_sets, results
        return self.__class__._json_tests[test_name]

    # Non-error cases

    def _match_results(self, testcase_name, assert_equality=None):
        partial_result_sets, expected = self._load_json_test(testcase_name)
        iterator = _MockCancellableIterator(*partial_result_sets)
        partial = self._make_one(iterator)
        if assert_equality is not None:
            assert_equality(list(partial), expected)
        else:
            self.assertEqual(list(partial), expected)

    def test_basic(self):
        self._match_results("Basic Test")

    def test_string_chunking(self):
        self._match_results("String Chunking Test")

    def test_string_array_chunking(self):
        self._match_results("String Array Chunking Test")

    def test_string_array_chunking_with_nulls(self):
        self._match_results("String Array Chunking Test With Nulls")

    def test_string_array_chunking_with_empty_strings(self):
        self._match_results("String Array Chunking Test With Empty Strings")

    def test_string_array_chunking_with_one_large_string(self):
        self._match_results("String Array Chunking Test With One Large String")

    def test_int64_array_chunking(self):
        self._match_results("INT64 Array Chunking Test")

    def test_float64_array_chunking(self):
        import math

        def assert_float_equality(lhs, rhs):
            # NaN, +Inf, and -Inf can't be tested for equality
            if lhs is None:
                self.assertIsNone(rhs)
            elif math.isnan(lhs):
                self.assertTrue(math.isnan(rhs))
            elif math.isinf(lhs):
                self.assertTrue(math.isinf(rhs))
                # but +Inf and -Inf can be tested for magnitude
                self.assertTrue((lhs > 0) == (rhs > 0))
            else:
                self.assertEqual(lhs, rhs)

        def assert_rows_equality(lhs, rhs):
            self.assertEqual(len(lhs), len(rhs))
            for l_rows, r_rows in zip(lhs, rhs):
                self.assertEqual(len(l_rows), len(r_rows))
                for l_row, r_row in zip(l_rows, r_rows):
                    self.assertEqual(len(l_row), len(r_row))
                    for l_cell, r_cell in zip(l_row, r_row):
                        assert_float_equality(l_cell, r_cell)

        self._match_results("FLOAT64 Array Chunking Test", assert_rows_equality)

    def test_struct_array_chunking(self):
        self._match_results("Struct Array Chunking Test")

    def test_nested_struct_array(self):
        self._match_results("Nested Struct Array Test")

    def test_nested_struct_array_chunking(self):
        self._match_results("Nested Struct Array Chunking Test")

    def test_struct_array_and_string_chunking(self):
        self._match_results("Struct Array And String Chunking Test")

    def test_multiple_row_single_chunk(self):
        self._match_results("Multiple Row Single Chunk")

    def test_multiple_row_multiple_chunks(self):
        self._match_results("Multiple Row Multiple Chunks")

    def test_multiple_row_chunks_non_chunks_interleaved(self):
        self._match_results("Multiple Row Chunks/Non Chunks Interleaved")


def _generate_partial_result_sets(prs_text_pbs):
    from google.cloud.spanner_v1 import PartialResultSet

    partial_result_sets = []

    for prs_text_pb in prs_text_pbs:
        prs = PartialResultSet.from_json(prs_text_pb)
        partial_result_sets.append(prs)

    return partial_result_sets


def _normalize_int_array(cell):
    normalized = []
    for subcell in cell:
        if subcell is not None:
            subcell = int(subcell)
        normalized.append(subcell)
    return normalized


def _normalize_float(cell):
    if cell == "Infinity":
        return float("inf")
    if cell == "-Infinity":
        return float("-inf")
    if cell == "NaN":
        return float("nan")
    if cell is not None:
        return float(cell)


def _normalize_results(rows_data, fields):
    """Helper for _parse_streaming_read_acceptance_tests"""
    from google.cloud.spanner_v1 import TypeCode

    normalized = []
    for row_data in rows_data:
        row = []
        assert len(row_data) == len(fields)
        for cell, field in zip(row_data, fields):
            if field.type_.code == TypeCode.INT64:
                cell = int(cell)
            if field.type_.code == TypeCode.FLOAT64:
                cell = _normalize_float(cell)
            elif field.type_.code == TypeCode.BYTES:
                cell = cell.encode("utf8")
            elif field.type_.code == TypeCode.ARRAY:
                if field.type_.array_element_type.code == TypeCode.INT64:
                    cell = _normalize_int_array(cell)
                elif field.type_.array_element_type.code == TypeCode.FLOAT64:
                    cell = [_normalize_float(subcell) for subcell in cell]
            row.append(cell)
        normalized.append(row)
    return normalized


def _parse_streaming_read_acceptance_tests(filename):
    """Parse acceptance tests from JSON

    See streaming-read-acceptance-test.json
    """
    import json

    with open(filename) as json_file:
        test_json = json.load(json_file)

    for test in test_json["tests"]:
        name = test["name"]
        partial_result_sets = _generate_partial_result_sets(test["chunks"])
        fields = partial_result_sets[0].metadata.row_type.fields
        result = _normalize_results(test["result"]["value"], fields)
        yield name, partial_result_sets, result
