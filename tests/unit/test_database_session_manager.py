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
from datetime import timedelta
from mock import Mock, patch
from os import environ
from threading import Thread
from time import time, sleep
from typing import Callable
from unittest import TestCase

from google.api_core.exceptions import BadRequest, FailedPrecondition
from google.cloud.spanner_v1.database_sessions_manager import DatabaseSessionsManager
from google.cloud.spanner_v1.session_options import SessionOptions, TransactionType
from tests._builders import build_database


# Shorten polling and refresh intervals for testing.
@patch.multiple(
    DatabaseSessionsManager,
    _MAINTENANCE_THREAD_POLLING_INTERVAL=timedelta(seconds=1),
    _MAINTENANCE_THREAD_REFRESH_INTERVAL=timedelta(seconds=2),
)
class TestDatabaseSessionManager(TestCase):
    def setUp(self):
        self._original_env = dict(environ)
        self._sessions_manager = self._build_sessions_manager()

    def tearDown(self):
        self._cleanup_database_sessions_manager()
        environ.clear()
        environ.update(self._original_env)

    def test_read_only_pooled(self):
        self._disable_multiplexed_sessions()
        manager = self._sessions_manager

        # Get session from pool.
        session = manager.get_session(TransactionType.READ_ONLY)
        self.assertFalse(session.is_multiplexed)
        manager._pool.get.assert_called_once()

        # Return session to pool.
        manager.put_session(session)
        manager._pool.put.assert_called_once_with(session)

    def test_read_only_multiplexed(self):
        self._enable_multiplexed_sessions()
        manager = self._sessions_manager

        # Session is created.
        session_1 = manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_1.is_multiplexed)
        manager.put_session(session_1)

        # Session is re-used.
        session_2 = manager.get_session(TransactionType.READ_ONLY)
        self.assertEqual(session_1, session_2)
        manager.put_session(session_2)

        # Verify that pool was not used.
        manager._pool.get.assert_not_called()
        manager._pool.put.assert_not_called()

        # Verify logger calls.
        info = manager._database.logger.info
        info.assert_called_once_with("Created multiplexed session.")

    def test_partitioned_pooled(self):
        self._disable_multiplexed_sessions()
        manager = self._sessions_manager

        # Get session from pool.
        session = manager.get_session(TransactionType.PARTITIONED)
        self.assertFalse(session.is_multiplexed)
        manager._pool.get.assert_called_once()

        # Return session to pool.
        manager.put_session(session)
        manager._pool.put.assert_called_once_with(session)

    def test_partitioned_multiplexed(self):
        self._enable_multiplexed_sessions()
        manager = self._sessions_manager

        # Session is created.
        session_1 = manager.get_session(TransactionType.PARTITIONED)
        self.assertTrue(session_1.is_multiplexed)
        manager.put_session(session_1)

        # Session is re-used.
        session_2 = manager.get_session(TransactionType.PARTITIONED)
        self.assertEqual(session_1, session_2)
        manager.put_session(session_2)

        # Verify that pool was not used.
        pool = manager._pool
        pool.get.assert_not_called()
        pool.put.assert_not_called()

        # Verify logger calls.
        info = manager._database.logger.info
        info.assert_called_once_with("Created multiplexed session.")

    def test_read_write_pooled(self):
        self._disable_multiplexed_sessions()
        manager = self._sessions_manager

        # Get session from pool.
        session = manager.get_session(TransactionType.READ_WRITE)
        self.assertFalse(session.is_multiplexed)
        manager._pool.get.assert_called_once()

        # Return session to pool.
        manager.put_session(session)
        manager._pool.put.assert_called_once_with(session)

    # TODO multiplexed: implement support for read/write transactions.
    def test_read_write_multiplexed(self):
        self._enable_multiplexed_sessions()

        with self.assertRaises(NotImplementedError):
            self._sessions_manager.get_session(TransactionType.READ_WRITE)

    def test_multiplexed_maintenance(self, *_):
        self._enable_multiplexed_sessions()
        manager = self._sessions_manager

        # Maintenance thread is started.
        session_1 = manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_1.is_multiplexed)
        self.assertTrue(manager._multiplexed_session_thread.is_alive())

        # Wait for maintenance thread to execute.
        self._assert_true_with_timeout(
            lambda: manager._database.spanner_api.create_session.call_count > 1
        )

        # Verify that maintenance thread created new multiplexed session.
        session_2 = manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_2.is_multiplexed)
        self.assertNotEqual(session_1, session_2)

    def test_multiplexed_maintenance_terminates_disabled(self):
        self._enable_multiplexed_sessions()
        manager = self._sessions_manager

        # Maintenance thread is started.
        session_1 = manager.get_session(TransactionType.READ_ONLY)
        self.assertTrue(session_1.is_multiplexed)

        manager._multiplexed_session_disabled_event.set()

        thread = manager._multiplexed_session_thread
        self._assert_thread_terminated(thread)

    def test_exception_bad_request(self):
        manager = self._sessions_manager
        api = manager._database.spanner_api
        api.create_session.side_effect = BadRequest("")

        # Verify that BadRequest is not caught.
        with self.assertRaises(BadRequest):
            manager.get_session(TransactionType.READ_ONLY)

    def test_exception_failed_precondition(self):
        manager = self._sessions_manager
        api = manager._database.spanner_api
        api.create_session.side_effect = FailedPrecondition("")

        # Verify that FailedPrecondition is not caught.
        with self.assertRaises(FailedPrecondition):
            manager.get_session(TransactionType.READ_ONLY)

    def _cleanup_database_sessions_manager(self) -> None:
        """Cleans up the database session manager after testing."""

        # If the maintenance thread is still alive, disable multiplexed sessions and
        # wait for the thread to terminate. We need to do this to ensure that the
        # thread is properly cleaned up and does not interfere with other tests.
        sessions_manager = self._sessions_manager
        thread = sessions_manager._multiplexed_session_thread

        if thread and thread.is_alive():
            sessions_manager._multiplexed_session_disabled_event.set()
            self._assert_thread_terminated(thread)

    def _assert_true_with_timeout(self, condition: Callable) -> None:
        """Asserts that the given condition is met within a timeout period."""

        sleep_seconds = 0.1
        timeout_seconds = 10

        start_time = time()
        while not condition() and time() - start_time < timeout_seconds:
            sleep(sleep_seconds)

        self.assertTrue(condition())

    def _assert_thread_terminated(self, thread: Thread) -> None:
        """Asserts that the given thread is terminated."""

        def _is_thread_terminated():
            return not thread.is_alive()

        self._assert_true_with_timeout(_is_thread_terminated)

    @staticmethod
    def _build_sessions_manager() -> DatabaseSessionsManager:
        """Builds and returns a new database session manager for testing.

        :rtype: :class:`~google.cloud.spanner_v1.database_sessions_manager.DatabaseSessionsManager`
        :returns: a new database session manager.
        """
        database = build_database()
        sessions_manager = database._sessions_manager

        # Mock the session pool.
        pool = sessions_manager._pool
        pool.get = Mock(wraps=pool.get)
        pool.put = Mock(wraps=pool.put)

        return sessions_manager

    @staticmethod
    def _disable_multiplexed_sessions() -> None:
        """Sets environment variables to disable multiplexed sessions for all transactions types."""
        environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "true"

    @staticmethod
    def _enable_multiplexed_sessions() -> None:
        """Sets environment variables to enable multiplexed sessions for all transaction types."""
        environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED] = "true"
        environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE] = "true"
        environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"
