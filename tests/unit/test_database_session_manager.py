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
from threading import Thread
from unittest import TestCase
from unittest.mock import Mock, DEFAULT, patch

from google.api_core.exceptions import (
    MethodNotImplemented,
    BadRequest,
    FailedPrecondition,
)

from google.cloud.spanner_v1.database_sessions_manager import DatabaseSessionsManager
from google.cloud.spanner_v1.session_options import TransactionType
from tests._helpers import disable_multiplexed_sessions, enable_multiplexed_sessions


# Shorten polling and refresh intervals for testing.
@patch.multiple(
    DatabaseSessionsManager,
    _MAINTENANCE_THREAD_POLLING_INTERVAL=datetime.timedelta(seconds=1),
    _MAINTENANCE_THREAD_REFRESH_INTERVAL=datetime.timedelta(seconds=2),
)
class TestDatabaseSessionManager(TestCase):
    def setUp(self):
        self._original_env = dict(os.environ)
        self._build_session_manager()

    def tearDown(self):
        self._cleanup_database_session_manager()
        os.environ.clear()
        os.environ.update(self._original_env)

    def test_read_only_pooled(self):
        disable_multiplexed_sessions()
        session_manager = self._session_manager

        # Get session from pool.
        session = session_manager.get_session(TransactionType.READ_ONLY)
        self.assertFalse(session.is_multiplexed)
        session_manager._pool.get.assert_called_once()

        # Return session to pool.
        session_manager.put_session(session)
        session_manager._pool.put.assert_called_once_with(session)

    def test_read_only_multiplexed(self):
        enable_multiplexed_sessions()
        session_manager = self._session_manager

        # Session is created.
        session_1 = session_manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_1.is_multiplexed)
        session_manager.put_session(session_1)

        # Session is re-used.
        session_2 = session_manager.get_session(TransactionType.READ_ONLY)
        self.assertEqual(session_1, session_2)
        session_manager.put_session(session_2)

        # Verify that pool was not used.
        session_manager._pool.get.assert_not_called()
        session_manager._pool.put.assert_not_called()

    def test_partitioned_pooled(self):
        disable_multiplexed_sessions()
        session_manager = self._session_manager

        # Get session from pool.
        session = session_manager.get_session(TransactionType.PARTITIONED)
        self.assertFalse(session.is_multiplexed)
        session_manager._pool.get.assert_called_once()

        # Return session to pool.
        session_manager.put_session(session)
        session_manager._pool.put.assert_called_once_with(session)

    def test_partitioned_multiplexed(self):
        enable_multiplexed_sessions()
        session_manager = self._session_manager

        # Session is created.
        session_1 = session_manager.get_session(TransactionType.PARTITIONED)
        self.assertTrue(session_1.is_multiplexed)
        session_manager.put_session(session_1)

        # Session is re-used.
        session_2 = session_manager.get_session(TransactionType.PARTITIONED)
        self.assertEqual(session_1, session_2)
        session_manager.put_session(session_2)

        # Verify that pool was not used.
        session_manager._pool.get.assert_not_called()
        session_manager._pool.put.assert_not_called()

    def test_read_write_pooled(self):
        disable_multiplexed_sessions()
        session_manager = self._session_manager

        # Get session from pool.
        session = session_manager.get_session(TransactionType.READ_WRITE)
        self.assertFalse(session.is_multiplexed)
        session_manager._pool.get.assert_called_once()

        # Return session to pool.
        session_manager.put_session(session)
        session_manager._pool.put.assert_called_once_with(session)

    def test_read_write_multiplexed(self):
        enable_multiplexed_sessions()
        session_manager = self._session_manager

        with self.assertRaises(NotImplementedError):
            session_manager.get_session(TransactionType.READ_WRITE)

    def test_multiplexed_maintenance(self, *_):
        enable_multiplexed_sessions()
        session_manager = self._session_manager

        # Maintenance thread is started.
        session_1 = session_manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_1.is_multiplexed)

        # Wait for maintenance thread to execute.
        api = session_manager._database.spanner_api

        def create_session_condition():
            return api.create_session.call_count > 1

        self._assert_true_with_timeout(create_session_condition)

        # Verify that maintenance thread created new multiplexed session.
        session_2 = session_manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_2.is_multiplexed)
        self.assertNotEqual(session_1, session_2)

    def test_multiplexed_maintenance_terminates_not_implemented(self):
        enable_multiplexed_sessions()
        session_manager = self._session_manager

        # Maintenance thread is started.
        session_1 = session_manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_1.is_multiplexed)

        # Multiplexed sessions not implemented.
        api = session_manager._database.spanner_api
        api.create_session.side_effect = MethodNotImplemented("test")

        # Verify that maintenance thread is terminated.
        thread = session_manager._multiplexed_session_maintenance_thread
        self._assert_thread_terminated(thread)

        # Verify that multiplexed session are disabled.
        session_options = session_manager._database.session_options
        self.assertFalse(session_options.use_multiplexed(TransactionType.READ_ONLY))
        self.assertFalse(session_options.use_multiplexed(TransactionType.PARTITIONED))
        self.assertFalse(session_options.use_multiplexed(TransactionType.READ_WRITE))

    def test_multiplexed_maintenance_terminates_disabled(self):
        enable_multiplexed_sessions()
        session_manager = self._session_manager

        # Maintenance thread is started.
        session_1 = session_manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_1.is_multiplexed)

        session_manager._is_multiplexed_sessions_disabled_event.set()

        thread = session_manager._multiplexed_session_maintenance_thread
        self._assert_thread_terminated(thread)

    def test_multiplexed_exception_method_not_implemented(self):
        enable_multiplexed_sessions()
        session_manager = self._session_manager

        # Multiplexed sessions not implemented.
        api = session_manager._database.spanner_api
        api.create_session.side_effect = [
            MethodNotImplemented("Test MethodNotImplemented"),
            DEFAULT,
        ]

        # Get session from pool.
        session = session_manager.get_session(TransactionType.READ_ONLY)
        self.assertFalse(session.is_multiplexed)
        session_manager._pool.get.assert_called_once()

        # Return session to pool.
        session_manager.put_session(session)
        session_manager._pool.put.assert_called_once_with(session)

        # Verify that multiplexed session are disabled.
        session_options = session_manager._database.session_options
        self.assertFalse(session_options.use_multiplexed(TransactionType.READ_ONLY))
        self.assertFalse(session_options.use_multiplexed(TransactionType.PARTITIONED))
        self.assertFalse(session_options.use_multiplexed(TransactionType.READ_WRITE))

    def test_exception_bad_request(self):
        session_manager = self._session_manager

        api = session_manager._database.spanner_api
        api.create_session.side_effect = BadRequest("test")

        # Verify that BadRequest is not caught.
        with self.assertRaises(BadRequest):
            session_manager.get_session(TransactionType.READ_ONLY)

    def test_exception_failed_precondition(self):
        session_manager = self._session_manager

        api = session_manager._database.spanner_api
        api.create_session.side_effect = FailedPrecondition("test")

        # Verify that FailedPrecondition is not caught.
        with self.assertRaises(FailedPrecondition):
            session_manager.get_session(TransactionType.READ_ONLY)

    def _build_session_manager(self) -> DatabaseSessionsManager:
        """Builds a new database session manager for testing."""
        from tests._builders import build_database

        database = build_database()
        session_manager = database._session_manager

        # Mock the session pool.
        pool = session_manager._pool
        pool.get = Mock(wraps=pool.get)
        pool.put = Mock(wraps=pool.put)

        self._session_manager = session_manager

    def _cleanup_database_session_manager(self) -> None:
        """Cleans up the database session manager after testing."""

        # If the maintenance thread is still alive, disable multiplexed sessions and
        # wait for the thread to terminate. We need to do this to ensure that the
        # thread is properly cleaned up and does not interfere with other tests.
        session_manager = self._session_manager
        thread = session_manager._multiplexed_session_maintenance_thread

        if thread and thread.is_alive():
            session_manager._is_multiplexed_sessions_disabled_event.set()
            self._assert_thread_terminated(thread)

    def _assert_true_with_timeout(self, condition):
        """Asserts that the given condition is met within a timeout period."""

        sleep_seconds = 0.1
        timeout_seconds = 10

        start_time = time.time()
        while not condition() and time.time() - start_time < timeout_seconds:
            time.sleep(sleep_seconds)

        self.assertTrue(condition())

    def _assert_thread_terminated(self, thread: Thread):
        """Asserts that the maintenance thread is terminated."""

        def _is_thread_terminated():
            return not thread.is_alive()

        self._assert_true_with_timeout(_is_thread_terminated)
