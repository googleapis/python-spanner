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

from google.cloud.spanner_admin_database_v1.types import spanner_database_admin
from google.cloud.spanner_v1 import (
    BatchCreateSessionsRequest,
    ExecuteSqlRequest,
    GetSessionRequest,
)

from tests.mockserver_tests.mock_server_test_base import (
    MockServerTestBase,
    add_select1_result,
)


class TestBasics(MockServerTestBase):
    def test_select1(self):
        add_select1_result()
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql("select 1")
            result_list = []
            for row in results:
                result_list.append(row)
                self.assertEqual(1, row[0])
            self.assertEqual(1, len(result_list))
        requests = self.spanner_service.requests
        self.assertEqual(3, len(requests))
        self.assertTrue(isinstance(requests[0], BatchCreateSessionsRequest))
        # TODO: Optimize FixedSizePool so this GetSessionRequest is not executed
        #       every time a session is fetched.
        self.assertTrue(isinstance(requests[1], GetSessionRequest))
        self.assertTrue(isinstance(requests[2], ExecuteSqlRequest))

    def test_create_table(self):
        database_admin_api = self.client.database_admin_api
        request = spanner_database_admin.UpdateDatabaseDdlRequest(
            dict(
                database=database_admin_api.database_path(
                    "test-project", "test-instance", "test-database"
                ),
                statements=[
                    "CREATE TABLE Test ("
                    "Id INT64, "
                    "Value STRING(MAX)) "
                    "PRIMARY KEY (Id)",
                ],
            )
        )
        operation = database_admin_api.update_database_ddl(request)
        operation.result(1)
