# Copyright 2025 Google LLC All rights reserved.
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
import os
from unittest import TestCase
from unittest.mock import Mock, patch

from google.cloud.spanner_v1 import SpannerClient
from google.cloud.spanner_v1.client import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.session import Session
from google.cloud.spanner_v1.session_options import SessionOptions


class TestDatabaseSessionManager(TestCase):
    def setUp(self):
        self._original_env = dict(os.environ)

        self._mocks = {
            "create_session": patch.object(SpannerClient, "create_session").start(),
            "get_session": patch.object(SpannerClient, "get_session").start(),
            "delete_session": patch.object(SpannerClient, "delete_session").start(),
        }

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._original_env)

        patch.stopall()

    def test_read_only_pooled(self):
        self._disable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        # Get session from pool.
        session = session_manager.get_session_for_read_only()
        self.assertFalse(session.is_multiplexed)
        session_manager._pool.get.assert_called_once()

        # Return session to pool.
        session_manager.put_session(session)
        session_manager._pool.put.assert_called_once_with(session)

    def test_read_only_multiplexed(self):
        self._enable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        with self.assertRaises(NotImplementedError):
            session_manager.get_session_for_read_only()

    def test_partitioned_non_multiplexed(self):
        self._disable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        # Get session from pool.
        session = session_manager.get_session_for_partitioned()
        self.assertFalse(session.is_multiplexed)
        session_manager._pool.get.assert_called_once()

        # Return session to pool.
        session_manager.put_session(session)
        session_manager._pool.put.assert_called_once_with(session)

    def test_partitioned_multiplexed(self):
        self._enable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        with self.assertRaises(NotImplementedError):
            session_manager.get_session_for_partitioned()

    def test_read_write_non_multiplexed(self):
        self._disable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        # Get session from pool.
        session = session_manager.get_session_for_read_write()
        self.assertFalse(session.is_multiplexed)
        session_manager._pool.get.assert_called_once()

        # Return session to pool.
        session_manager.put_session(session)
        session_manager._pool.put.assert_called_once_with(session)

    def test_read_write_multiplexed(self):
        self._enable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        with self.assertRaises(NotImplementedError):
            session_manager.get_session_for_read_write()

    def test_put_multiplexed(self):
        session_manager = self._build_database_session_manager()

        with self.assertRaises(NotImplementedError):
            session_manager.put_session(
                Session(database=session_manager._database, is_multiplexed=True)
            )

    @staticmethod
    def _build_database_session_manager():
        """Builds and returns a new database session manager for testing."""

        client = Client(project="project-id")
        instance = Instance(instance_id="instance-id", client=client)

        database = Database(database_id="database-id", instance=instance)
        session_manager = database._session_manager

        # Mock the session pool.
        pool = session_manager._pool
        pool.get = Mock(wraps=pool.get)
        pool.put = Mock(wraps=pool.put)

        return session_manager

    @staticmethod
    def _enable_multiplexed_env_vars():
        """Sets environment variables to enable multiplexed sessions."""

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED] = "true"
        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE] = "true"
        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"

    @staticmethod
    def _disable_multiplexed_env_vars():
        """Sets environment variables to disable multiplexed sessions."""

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "true"
