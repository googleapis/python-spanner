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
import datetime
import time
from unittest import TestCase
from unittest.mock import Mock, patch, DEFAULT, PropertyMock

from google.api_core.exceptions import (
    MethodNotImplemented,
    BadRequest,
    FailedPrecondition,
)

from google.cloud.spanner_v1 import SpannerClient
from google.cloud.spanner_v1.client import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.database_sessions_manager import DatabaseSessionsManager
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.session_options import SessionOptions


class TestDatabaseSessionManager(TestCase):
    def setUp(self):
        self._original_env = dict(os.environ)

        self._mocks = {
            "create_session": patch.object(SpannerClient, "create_session").start(),
            "delete_session": patch.object(SpannerClient, "delete_session").start(),
            # Mock faster polling and refresh intervals for tests.
            "polling_interval": patch.object(
                DatabaseSessionsManager,
                "_MAINTENANCE_THREAD_POLLING_INTERVAL",
                new_callable=PropertyMock,
                return_value=datetime.timedelta(seconds=1),
            ).start(),
            "refresh_interval": patch.object(
                DatabaseSessionsManager,
                "_MAINTENANCE_THREAD_REFRESH_INTERVAL",
                new_callable=PropertyMock,
                return_value=datetime.timedelta(seconds=2),
            ).start(),
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

        # Session is created.
        session_1 = session_manager.get_session_for_read_only()
        self.assertTrue(session_1.is_multiplexed)
        session_manager.put_session(session_1)

        # Session is re-used.
        session_2 = session_manager.get_session_for_read_only()
        self.assertEqual(session_1, session_2)
        session_manager.put_session(session_2)

        # Verify that pool was not used.
        session_manager._pool.get.assert_not_called()
        session_manager._pool.put.assert_not_called()

    def test_partitioned_pooled(self):
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

    def test_read_write_pooled(self):
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

    def test_multiplexed_maintenance(self):
        self._enable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        # Maintenance thread is started.
        session_1 = session_manager.get_session_for_read_only()
        self.assertTrue(session_1.is_multiplexed)

        # Wait for maintenance thread to execute.
        def create_session_condition():
            return self._mocks["create_session"].call_count > 1

        self.assert_true_with_timeout(create_session_condition)

        # Verify that maintenance thread created new multiplexed session.
        session_2 = session_manager.get_session_for_read_only()
        self.assertTrue(session_2.is_multiplexed)
        self.assertNotEqual(session_1, session_2)

    def test_multiplexed_maintenance_terminates_not_implemented(self):
        self._enable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        # Maintenance thread is started.
        session_1 = session_manager.get_session_for_read_only()
        self.assertTrue(session_1.is_multiplexed)

        # Multiplexed sessions not implemented.
        create_session_mock = self._mocks["create_session"]
        create_session_mock.side_effect = MethodNotImplemented(
            "Multiplexed sessions not implemented"
        )

        # Wait for maintenance thread to terminate.
        thread = session_manager._multiplexed_session_maintenance_thread

        def thread_terminated_condition():
            return not thread.is_alive()

        self.assert_true_with_timeout(thread_terminated_condition)

        # Verify that multiplexed sessions are disabled.
        session_options = session_manager._database._instance._client.session_options
        self.assertFalse(session_options.use_multiplexed_for_read_only())
        self.assertFalse(session_options.use_multiplexed_for_partitioned())
        self.assertFalse(session_options.use_multiplexed_for_read_write())

    def test_multiplexed_maintenance_terminates_disabled(self):
        self._enable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        # Maintenance thread is started.
        session_1 = session_manager.get_session_for_read_only()
        self.assertTrue(session_1.is_multiplexed)

        session_manager._is_multiplexed_sessions_disabled_event.set()

        # Wait for maintenance thread to terminate.
        thread = session_manager._multiplexed_session_maintenance_thread

        def thread_terminated_condition():
            return not thread.is_alive()

        self.assert_true_with_timeout(thread_terminated_condition)

    def test_multiplexed_exception_method_not_implemented(self):
        self._enable_multiplexed_env_vars()
        session_manager = self._build_database_session_manager()

        # Multiplexed sessions not implemented.
        self._mocks["create_session"].side_effect = [
            MethodNotImplemented("Test MethodNotImplemented"),
            DEFAULT,
        ]

        # Get session from pool.
        session = session_manager.get_session_for_read_only()
        self.assertFalse(session.is_multiplexed)
        session_manager._pool.get.assert_called_once()

        # Return session to pool.
        session_manager.put_session(session)
        session_manager._pool.put.assert_called_once_with(session)

        # Verify that multiplexed session are disabled.
        session_options = session_manager._database._instance._client.session_options
        self.assertFalse(session_options.use_multiplexed_for_read_only())
        self.assertFalse(session_options.use_multiplexed_for_partitioned())
        self.assertFalse(session_options.use_multiplexed_for_read_write())

    def test_exception_bad_request(self):
        session_manager = self._build_database_session_manager()

        # Verify that BadRequest is not caught.
        with self.assertRaises(BadRequest):
            self._mocks["create_session"].side_effect = BadRequest("Test BadRequest")
            session_manager.get_session_for_read_only()

    def test_exception_failed_precondition(self):
        session_manager = self._build_database_session_manager()

        # Verify that FailedPrecondition is not caught.
        with self.assertRaises(FailedPrecondition):
            self._mocks["create_session"].side_effect = FailedPrecondition(
                "Test FailedPrecondition"
            )
            session_manager.get_session_for_read_only()

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

    @staticmethod
    def assert_true_with_timeout(condition):
        """Asserts that the given condition is met within a timeout period."""

        sleep_seconds = 0.1
        timeout_seconds = 10

        start_time = time.time()
        while not condition() and time.time() - start_time < timeout_seconds:
            time.sleep(sleep_seconds)

        assert condition()
