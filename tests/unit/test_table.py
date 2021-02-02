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

import mock


class _BaseTest(unittest.TestCase):
    TABLE_ID = "test_table"

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)


class TestTable(_BaseTest):
    def _get_target_class(self):
        from google.cloud.spanner_v1.table import Table

        return Table

    def test_ctor(self):
        from google.cloud.spanner_v1.database import Database

        db = mock.create_autospec(Database, instance=True)
        table = self._make_one(self.TABLE_ID, db)
        self.assertEqual(table.table_id, self.TABLE_ID)

    def test_get_schema(self):
        from google.cloud.spanner_v1.database import Database, SnapshotCheckout
        from google.cloud.spanner_v1.snapshot import Snapshot
        from google.cloud.spanner_v1.table import _GET_SCHEMA_TEMPLATE

        db = mock.create_autospec(Database, instance=True)
        checkout = mock.create_autospec(SnapshotCheckout, instance=True)
        snapshot = mock.create_autospec(Snapshot, instance=True)
        db.snapshot.return_value = checkout
        checkout.__enter__.return_value = snapshot
        table = self._make_one(self.TABLE_ID, db)
        schema = table.get_schema()
        self.assertIsInstance(schema, list)
        expected_query = _GET_SCHEMA_TEMPLATE.format(self.TABLE_ID)
        snapshot.execute_sql.assert_called_with(expected_query)
