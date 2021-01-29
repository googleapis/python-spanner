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

"""User friendly container for Cloud Spanner Table."""

_GET_SCHEMA_TEMPLATE = "SELECT * FROM {} LIMIT 0"


class Table(object):
    """Representation of a Cloud Spanner Table.
    """

    def __init__(self, table_id, database):
        self.table_id = table_id
        self._database = database

    def get_schema(self):
        """
        List of google.cloud.spanner_v1.types.Field
        """
        with self._database.snapshot() as snapshot:
            query = _GET_SCHEMA_TEMPLATE.format(self.table_id)
            results = snapshot.execute_sql(query)
            # _ = list(results)
            return list(results.fields)
