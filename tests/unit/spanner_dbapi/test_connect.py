# Copyright 2020 Google LLC
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

"""connect() module function unit tests."""

import unittest
from unittest import mock

from google.auth.credentials import AnonymousCredentials

from tests._builders import build_scoped_credentials

INSTANCE = "test-instance"
DATABASE = "test-database"
PROJECT = "test-project"
USER_AGENT = "user-agent"


@mock.patch("google.cloud.spanner_v1.Client")
class Test_connect(unittest.TestCase):
    def test_w_implicit(self, mock_client):
        from google.cloud.spanner_dbapi import connect
        from google.cloud.spanner_dbapi import Connection

        client = mock_client.return_value
        instance = client.instance.return_value
        database = instance.database.return_value

        connection = connect(
            "test-instance",
            "test-database",
            project="test-project",
            credentials=AnonymousCredentials(),
            client_options={"api_endpoint": "none"},
        )

        self.assertIsInstance(connection, Connection)

        self.assertIs(connection.instance, instance)
        client.instance.assert_called_once_with(INSTANCE)
        mock_client.assert_called_once_with(
            project=mock.ANY,
            credentials=mock.ANY,
            client_info=mock.ANY,
            client_options=mock.ANY,
            route_to_leader_enabled=True,
        )

        self.assertIs(connection.database, database)
        instance.database.assert_called_once_with(
            DATABASE, pool=None, database_role=None
        )
        # Database constructs its own pool
        self.assertIsNotNone(connection.database._pool)
        self.assertTrue(connection.instance._client.route_to_leader_enabled)

    def test_w_explicit(self, mock_client):
        from google.cloud.spanner_v1.pool import AbstractSessionPool
        from google.cloud.spanner_dbapi import connect
        from google.cloud.spanner_dbapi import Connection
        from google.cloud.spanner_dbapi.version import PY_VERSION

        credentials = build_scoped_credentials()
        pool = mock.create_autospec(AbstractSessionPool)
        client = mock_client.return_value
        instance = client.instance.return_value
        database = instance.database.return_value
        role = "some_role"

        connection = connect(
            INSTANCE,
            DATABASE,
            PROJECT,
            credentials,
            pool=pool,
            database_role=role,
            user_agent=USER_AGENT,
            route_to_leader_enabled=False,
        )

        self.assertIsInstance(connection, Connection)

        mock_client.assert_called_once_with(
            project=PROJECT,
            credentials=credentials,
            client_info=mock.ANY,
            client_options=mock.ANY,
            route_to_leader_enabled=False,
        )
        client_info = mock_client.call_args_list[0][1]["client_info"]
        self.assertEqual(client_info.user_agent, USER_AGENT)
        self.assertEqual(client_info.python_version, PY_VERSION)

        self.assertIs(connection.instance, instance)
        client.instance.assert_called_once_with(INSTANCE)

        self.assertIs(connection.database, database)
        instance.database.assert_called_once_with(
            DATABASE, pool=pool, database_role=role
        )

    def test_w_credential_file_path(self, mock_client):
        from google.cloud.spanner_dbapi import connect
        from google.cloud.spanner_dbapi import Connection
        from google.cloud.spanner_dbapi.version import PY_VERSION

        credentials_path = "dummy/file/path.json"

        connection = connect(
            INSTANCE,
            DATABASE,
            PROJECT,
            credentials=credentials_path,
            user_agent=USER_AGENT,
        )

        self.assertIsInstance(connection, Connection)

        factory = mock_client.from_service_account_json
        factory.assert_called_once_with(
            credentials_path,
            project=PROJECT,
            client_info=mock.ANY,
            route_to_leader_enabled=True,
        )
        client_info = factory.call_args_list[0][1]["client_info"]
        self.assertEqual(client_info.user_agent, USER_AGENT)
        self.assertEqual(client_info.python_version, PY_VERSION)

    def test_with_kwargs(self, mock_client):
        from google.cloud.spanner_dbapi import connect
        from google.cloud.spanner_dbapi import Connection

        client = mock_client.return_value
        instance = client.instance.return_value
        database = instance.database.return_value
        self.assertIsNotNone(database)

        connection = connect(INSTANCE, DATABASE, ignore_transaction_warnings=True)

        self.assertIsInstance(connection, Connection)
        self.assertTrue(connection._ignore_transaction_warnings)
