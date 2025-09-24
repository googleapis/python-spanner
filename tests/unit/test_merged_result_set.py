# Copyright 2024 Google LLC All rights reserved.
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


class TestMergedResultSet(unittest.TestCase):
    def _get_target_class(self):
        from google.cloud.spanner_v1.merged_result_set import MergedResultSet

        return MergedResultSet

    def _make_one(self, *args, **kwargs):
        klass = self._get_target_class()
        obj = super(klass, klass).__new__(klass)
        from threading import Event, Lock

        obj.metadata_event = Event()
        obj.metadata_lock = Lock()
        obj._metadata = None
        return obj

    @staticmethod
    def _make_value(value):
        from google.cloud.spanner_v1._helpers import _make_value_pb

        return _make_value_pb(value)

    @staticmethod
    def _make_scalar_field(name, type_):
        from google.cloud.spanner_v1 import StructType
        from google.cloud.spanner_v1 import Type

        return StructType.Field(name=name, type_=Type(code=type_))

    @staticmethod
    def _make_result_set_metadata(fields=()):
        from google.cloud.spanner_v1 import ResultSetMetadata
        from google.cloud.spanner_v1 import StructType

        metadata = ResultSetMetadata(row_type=StructType(fields=[]))
        for field in fields:
            metadata.row_type.fields.append(field)
        return metadata

    def test_decoders_property_no_metadata(self):
        merged = self._make_one()
        merged._metadata = None
        merged.metadata_event.set()
        with self.assertRaises(ValueError):
            getattr(merged, "_decoders")

    def test_decoders_property_with_metadata(self):
        from google.cloud.spanner_v1 import TypeCode

        merged = self._make_one()
        fields = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
        ]
        merged._metadata = self._make_result_set_metadata(fields)
        merged.metadata_event.set()

        decoders = merged._decoders
        self.assertEqual(len(decoders), 2)
        self.assertTrue(callable(decoders[0]))
        self.assertTrue(callable(decoders[1]))

    def test_decode_row(self):
        from google.cloud.spanner_v1 import TypeCode

        merged = self._make_one()
        fields = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
        ]
        merged._metadata = self._make_result_set_metadata(fields)
        merged.metadata_event.set()

        raw_row = [self._make_value("Phred"), self._make_value(42)]
        decoded_row = merged.decode_row(raw_row)

        self.assertEqual(decoded_row, ["Phred", 42])

    def test_decode_row_type_error(self):
        merged = self._make_one()
        # The _decoders property requires metadata, even for a type error check.
        merged._metadata = self._make_result_set_metadata()
        merged.metadata_event.set()
        with self.assertRaises(TypeError):
            merged.decode_row("not a list")

    def test_decode_column(self):
        from google.cloud.spanner_v1 import TypeCode

        merged = self._make_one()
        fields = [
            self._make_scalar_field("full_name", TypeCode.STRING),
            self._make_scalar_field("age", TypeCode.INT64),
        ]
        merged._metadata = self._make_result_set_metadata(fields)
        merged.metadata_event.set()

        raw_row = [self._make_value("Phred"), self._make_value(42)]
        decoded_name = merged.decode_column(raw_row, 0)
        decoded_age = merged.decode_column(raw_row, 1)

        self.assertEqual(decoded_name, "Phred")
        self.assertEqual(decoded_age, 42)

    def test_decode_column_type_error(self):
        merged = self._make_one()
        # The _decoders property requires metadata, even for a type error check.
        merged._metadata = self._make_result_set_metadata()
        merged.metadata_event.set()
        with self.assertRaises(TypeError):
            merged.decode_column("not a list", 0)
