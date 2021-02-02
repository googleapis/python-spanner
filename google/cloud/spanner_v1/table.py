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

    :type table_id: str
    :param table_id: The ID of the table.

    :type database: :class:`~google.cloud.spanner_v1.database.Database`
    :param database: The database that owns the table.
    """

    def __init__(self, table_id, database):
        self._table_id = table_id
        self._database = database

    @property
    def table_id(self):
        """The ID of the table used in SQL.

        :rtype: str
        :returns: The table ID.
        """
        return self._table_id

    def get_schema(self):
        """Get the schema of this table.

        :rtype: list of :class:`~google.cloud.spanner_v1.types.StructType.Field`
        :returns: The table schema.
        """
        with self._database.snapshot() as snapshot:
            query = _GET_SCHEMA_TEMPLATE.format(self.table_id)
            results = snapshot.execute_sql(query)
            # Start iterating to force the schema to download.
            try:
                next(iter(results))
            except StopIteration:
                pass
            return list(results.fields)
