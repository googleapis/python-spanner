# Copyright 2016 Google LLC All rights reserved.
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


from functools import total_ordering
import unittest
import random
import string
import threading
import mock
import datetime


def _make_database(name="name"):
    from google.cloud.spanner_v1.database import Database

    return mock.create_autospec(Database, instance=True)


def _make_session():
    from google.cloud.spanner_v1.database import Session

    return mock.create_autospec(Session, instance=True)


class TestAbstractSessionPool(unittest.TestCase):
    def _getTargetClass(self):
        from google.cloud.spanner_v1.pool import AbstractSessionPool

        return AbstractSessionPool

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_ctor_defaults(self):
        pool = self._make_one()
        self.assertIsNone(pool._database)
        self.assertEqual(pool.labels, {})
        self.assertIsNone(pool.database_role)

    def test_ctor_explicit(self):
        labels = {"foo": "bar"}
        database_role = "dummy-role"
        pool = self._make_one(labels=labels, database_role=database_role)
        self.assertIsNone(pool._database)
        self.assertEqual(pool.labels, labels)
        self.assertEqual(pool.database_role, database_role)

    def test_bind_abstract(self):
        pool = self._make_one()
        database = _make_database("name")
        with self.assertRaises(NotImplementedError):
            pool.bind(database)

    def test_get_abstract(self):
        pool = self._make_one()
        with self.assertRaises(NotImplementedError):
            pool.get()

    def test_put_abstract(self):
        pool = self._make_one()
        session = object()
        with self.assertRaises(NotImplementedError):
            pool.put(session)

    def test_clear_abstract(self):
        pool = self._make_one()
        with self.assertRaises(NotImplementedError):
            pool.clear()

    def test__new_session_wo_labels(self):
        pool = self._make_one()
        database = pool._database = _make_database("name")
        session = _make_session()
        database.session.return_value = session

        new_session = pool._new_session()

        self.assertIs(new_session, session)
        database.session.assert_called_once_with(labels={}, database_role=None)

    def test__new_session_w_labels(self):
        labels = {"foo": "bar"}
        pool = self._make_one(labels=labels)
        database = pool._database = _make_database("name")
        session = _make_session()
        database.session.return_value = session

        new_session = pool._new_session()

        self.assertIs(new_session, session)
        database.session.assert_called_once_with(labels=labels, database_role=None)

    def test__new_session_w_database_role(self):
        database_role = "dummy-role"
        pool = self._make_one(database_role=database_role)
        database = pool._database = _make_database("name")
        session = _make_session()
        database.session.return_value = session

        new_session = pool._new_session()

        self.assertIs(new_session, session)
        database.session.assert_called_once_with(labels={}, database_role=database_role)

    def test_session_wo_kwargs(self):
        from google.cloud.spanner_v1.pool import SessionCheckout

        pool = self._make_one()
        checkout = pool.session()
        self.assertIsInstance(checkout, SessionCheckout)
        self.assertIs(checkout._pool, pool)
        self.assertIsNone(checkout._session)
        self.assertEqual(checkout._kwargs, {})

    def test_session_w_kwargs(self):
        from google.cloud.spanner_v1.pool import SessionCheckout

        pool = self._make_one()
        checkout = pool.session(foo="bar")
        self.assertIsInstance(checkout, SessionCheckout)
        self.assertIs(checkout._pool, pool)
        self.assertIsNone(checkout._session)
        self.assertEqual(checkout._kwargs, {"foo": "bar"})

    @mock.patch("threading.Thread")
    def test_startCleaningLongRunningSessions_success(self, mock_thread_class):
        mock_thread = mock.MagicMock()
        mock_thread.start = mock.MagicMock()
        mock_thread_class.return_value = mock_thread

        pool = self._make_one()
        pool._database = mock.MagicMock()
        pool._cleanup_task_ongoing_event.clear()
        with mock.patch(
            "google.cloud.spanner_v1._helpers.DELETE_LONG_RUNNING_TRANSACTION_FREQUENCY_SEC",
            new=5,
        ), mock.patch(
            "google.cloud.spanner_v1._helpers.DELETE_LONG_RUNNING_TRANSACTION_THRESHOLD_SEC",
            new=10,
        ):
            pool.startCleaningLongRunningSessions()

            # The event should be set, indicating the task is now ongoing
            self.assertTrue(pool._cleanup_task_ongoing_event.is_set())

            # A new thread should have been created to start the task
            threading.Thread.assert_called_once_with(
                target=pool.deleteLongRunningTransactions,
                args=(5, 10),
                daemon=True,
                name="recycle-sessions",
            )
            mock_thread.start.assert_called_once()
            pool.stopCleaningLongRunningSessions()
            self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    # @mock.patch("threading.Thread")
    # def test_startCleaningLongRunningSessions_should_trigger_background_task_once(self, mock_thread_class):
    #     mock_thread = mock.MagicMock()
    #     mock_thread.start = mock.MagicMock()
    #     mock_thread_class.return_value = mock_thread

    #     pool = self._make_one()
    #     pool._database = mock.MagicMock()
    #     pool._cleanup_task_ongoing_event.clear()
    #     with mock.patch(
    #         "google.cloud.spanner_v1._helpers.DELETE_LONG_RUNNING_TRANSACTION_FREQUENCY_SEC",
    #         new=5,
    #     ), mock.patch(
    #         "google.cloud.spanner_v1._helpers.DELETE_LONG_RUNNING_TRANSACTION_THRESHOLD_SEC",
    #         new=10,
    #     ):
    #         pool.startCleaningLongRunningSessions()
    #         threads = []
    #         threads.append(
    #             threading.Thread(
    #                 target=pool.startCleaningLongRunningSessions,
    #             )
    #         )
    #         threads.append(
    #             threading.Thread(
    #                 target=pool.startCleaningLongRunningSessions,
    #             )
    #         )
    #         for thread in threads:
    #             thread.start()

    #         for thread in threads:
    #             thread.join()

    #         # A new thread should have been created to start the task
    #         threading.Thread.assert_called_once_with(
    #             target=pool.deleteLongRunningTransactions,
    #             args=(5, 10),
    #             daemon=True,
    #             name="recycle-sessions",
    #         )

    #     mock_thread.start.assert_called_once()
    #     pool.stopCleaningLongRunningSessions()
    #     self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def test_stopCleaningLongRunningSessions(self):
        pool = self._make_one()
        pool._cleanup_task_ongoing_event.set()
        pool.stopCleaningLongRunningSessions()

        # The event should not be set, indicating the task is now stopped
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def _setup_session_leak(self, close_inactive_transactions, logging_enabled):
        pool = self._make_one()
        pool._database = mock.MagicMock()
        pool.put = mock.MagicMock()

        def put_side_effect(*args, **kwargs):
            pool._borrowed_sessions = []
            pool._cleanup_task_ongoing_event.clear()

        pool.put.side_effect = put_side_effect

        pool._database.logging_enabled = logging_enabled
        pool._cleanup_task_ongoing_event.set()
        pool._database.close_inactive_transactions = close_inactive_transactions
        pool._borrowed_sessions = []
        pool._database.logger.warning = mock.MagicMock()
        pool._format_trace = mock.MagicMock()
        return pool

    def test_deleteLongRunningTransactions_noSessionsToDelete(self):
        pool = self._setup_session_leak(True, True)
        pool.deleteLongRunningTransactions(1, 1)

        # Assert that no warnings were logged and no sessions were put back
        self.assertEqual(pool._database.logger.warning.call_count, 0)
        pool.put.assert_not_called()
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def test_deleteLongRunningTransactions_deleteAndLogSession(self):
        pool = self._setup_session_leak(True, True)
        # Create a session that needs to be closed
        session = mock.MagicMock()
        session.transaction_logged = False
        session.checkout_time = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=61
        )
        session.long_running = False
        session._session_id = "session_id"
        pool._traces["session_id"] = "trace"
        pool._borrowed_sessions = [session]
        pool._cleanup_task_ongoing_event.set()
        # Call deleteLongRunningTransactions
        pool.deleteLongRunningTransactions(2, 2)

        # Assert that the session was put back and a warning was logged
        pool.put.assert_called_once()
        pool._database.logger.warning.assert_called_once()
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def test_deleteLongRunningTransactions_logSession(self):
        pool = self._setup_session_leak(False, True)
        # Create a session that needs to be closed
        session = mock.MagicMock()
        session.transaction_logged = False
        session.checkout_time = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=61
        )
        session.long_running = False
        session._session_id = "session_id"
        pool._traces["session_id"] = "trace"
        pool._borrowed_sessions = [session]
        pool._cleanup_task_ongoing_event.set()
        # Call deleteLongRunningTransactions
        pool.deleteLongRunningTransactions(2, 2)

        # Assert that the session was not put back and a warning was logged
        pool.put.assert_not_called()
        pool._database.logger.warning.assert_called_once()
        self.assertTrue(session.transaction_logged)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def test_deleteLongRunningTransactions_deleteSession(self):
        pool = self._setup_session_leak(True, False)
        # Create a session that needs to be closed
        session = mock.MagicMock()
        session.transaction_logged = False
        session.checkout_time = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=61
        )
        session.long_running = False
        session._session_id = "session_id"
        pool._traces["session_id"] = "trace"
        pool._borrowed_sessions = [session]
        pool._cleanup_task_ongoing_event.set()
        # Call deleteLongRunningTransactions
        pool.deleteLongRunningTransactions(2, 2)

        # Assert that the session was not put back and a warning was logged
        pool.put.assert_called_once()
        pool._database.logger.warning.assert_not_called()
        self.assertFalse(session.transaction_logged)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def test_deleteLongRunningTransactions_close_if_no_transaction_is_released(self):
        pool = self._setup_session_leak(False, True)
        # Create a session that needs to be closed
        session = mock.MagicMock()
        session.transaction_logged = True
        session.checkout_time = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=61
        )
        session.long_running = False
        session._session_id = "session_id"
        pool._traces["session_id"] = "trace"
        pool._borrowed_sessions = [session]
        pool._cleanup_task_ongoing_event.set()
        # Call deleteLongRunningTransactions
        pool.deleteLongRunningTransactions(2, 2)

        # Assert that background task was closed as there was no transaction to close.
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())


class TestFixedSizePool(unittest.TestCase):
    def _getTargetClass(self):
        from google.cloud.spanner_v1.pool import FixedSizePool

        return FixedSizePool

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_ctor_defaults(self):
        pool = self._make_one()
        self.assertIsNone(pool._database)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.default_timeout, 10)
        self.assertTrue(pool._sessions.empty())
        self.assertEqual(pool.labels, {})
        self.assertIsNone(pool.database_role)

    def test_ctor_explicit(self):
        labels = {"foo": "bar"}
        database_role = "dummy-role"
        pool = self._make_one(
            size=4, default_timeout=30, labels=labels, database_role=database_role
        )
        self.assertIsNone(pool._database)
        self.assertEqual(pool.size, 4)
        self.assertEqual(pool.default_timeout, 30)
        self.assertTrue(pool._sessions.empty())
        self.assertEqual(pool.labels, labels)
        self.assertEqual(pool.database_role, database_role)

    def test_bind(self):
        database_role = "dummy-role"
        pool = self._make_one()
        database = _Database("name")
        SESSIONS = [_Session(database)] * 10
        database._database_role = database_role
        database._sessions.extend(SESSIONS)

        pool.bind(database)

        self.assertIs(pool._database, database)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.database_role, database_role)
        self.assertEqual(pool.default_timeout, 10)
        self.assertTrue(pool._sessions.full())

        api = database.spanner_api
        self.assertEqual(api.batch_create_sessions.call_count, 5)
        for session in SESSIONS:
            session.create.assert_not_called()

    def test_get_non_expired(self):
        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = sorted([_Session(database) for i in range(0, 4)])
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        # check if sessions returned in LIFO order
        for i in (3, 2, 1, 0):
            session = pool.get()
            self.assertIs(session, SESSIONS[i])
            self.assertTrue(session._exists_checked)
            self.assertFalse(pool._sessions.full())
        # Stop Long running session
        pool.stopCleaningLongRunningSessions()

    def test_get_expired(self):
        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 5
        SESSIONS[0]._exists = False
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        session = pool.get()

        self.assertIs(session, SESSIONS[4])
        session.create.assert_called()
        self.assertTrue(SESSIONS[0]._exists_checked)
        self.assertFalse(pool._sessions.full())
        pool.stopCleaningLongRunningSessions()

    def test_get_trigger_longrunning_and_set_defaults(self):
        pool = self._make_one(size=2)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 3
        for session in SESSIONS:
            session._exists = True
        database._sessions.extend(SESSIONS)
        pool.bind(database)
        session = pool.get()
        self.assertIsInstance(session.checkout_time, datetime.datetime)
        self.assertFalse(session.long_running)
        self.assertFalse(session.transaction_logged)
        self.assertIs(session, pool._borrowed_sessions[0])
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

        # Fetch new session which will trigger the cleanup task.
        pool.get()
        self.assertTrue(pool._cleanup_task_ongoing_event.is_set())

        pool.stopCleaningLongRunningSessions()
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def test_get_empty_default_timeout(self):
        import queue

        pool = self._make_one(size=1)
        session_queue = pool._sessions = _Queue()

        with self.assertRaises(queue.Empty):
            pool.get()

        self.assertEqual(session_queue._got, {"block": True, "timeout": 10})

    def test_get_empty_explicit_timeout(self):
        import queue

        pool = self._make_one(size=1, default_timeout=0.1)
        session_queue = pool._sessions = _Queue()

        with self.assertRaises(queue.Empty):
            pool.get(timeout=1)

        self.assertEqual(session_queue._got, {"block": True, "timeout": 1})

    def test_put_full(self):
        import queue

        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 4
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        with self.assertRaises(queue.Full):
            pool.put(_Session(database))

        self.assertTrue(pool._sessions.full())

    def test_put_non_full(self):
        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 4
        database._sessions.extend(SESSIONS)
        pool.bind(database)
        pool._sessions.get()

        session = _Session(database)
        pool._borrowed_sessions.append(session)
        pool._cleanup_task_ongoing_event.set()

        pool.put(session)

        self.assertEqual(len(pool._borrowed_sessions), 0)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())
        self.assertTrue(pool._sessions.full())

    def test_clear(self):
        pool = self._make_one()
        database = _Database("name")
        SESSIONS = [_Session(database)] * 10
        database._sessions.extend(SESSIONS)
        pool.bind(database)
        self.assertTrue(pool._sessions.full())

        api = database.spanner_api
        self.assertEqual(api.batch_create_sessions.call_count, 5)
        for session in SESSIONS:
            session.create.assert_not_called()

        pool.clear()

        for session in SESSIONS:
            self.assertTrue(session._deleted)


class TestBurstyPool(unittest.TestCase):
    def _getTargetClass(self):
        from google.cloud.spanner_v1.pool import BurstyPool

        return BurstyPool

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_ctor_defaults(self):
        pool = self._make_one()
        self.assertIsNone(pool._database)
        self.assertEqual(pool.target_size, 10)
        self.assertTrue(pool._sessions.empty())
        self.assertEqual(pool.labels, {})
        self.assertIsNone(pool.database_role)

    def test_ctor_explicit(self):
        labels = {"foo": "bar"}
        database_role = "dummy-role"
        pool = self._make_one(target_size=4, labels=labels, database_role=database_role)
        self.assertIsNone(pool._database)
        self.assertEqual(pool.target_size, 4)
        self.assertTrue(pool._sessions.empty())
        self.assertEqual(pool.labels, labels)
        self.assertEqual(pool.database_role, database_role)

    def test_ctor_explicit_w_database_role_in_db(self):
        database_role = "dummy-role"
        pool = self._make_one()
        database = pool._database = _Database("name")
        database._database_role = database_role
        pool.bind(database)
        self.assertEqual(pool.database_role, database_role)

    def test_get_empty(self):
        pool = self._make_one()
        database = _Database("name")
        database._sessions.append(_Session(database))
        pool.bind(database)

        session = pool.get()

        self.assertIsInstance(session, _Session)
        self.assertIs(session._database, database)
        session.create.assert_called()
        self.assertTrue(pool._sessions.empty())

    def test_get_trigger_longrunning_and_set_defaults(self):
        pool = self._make_one(target_size=2)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 3
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        session = pool.get()
        session.create.assert_called()
        self.assertIsInstance(session.checkout_time, datetime.datetime)
        self.assertFalse(session.long_running)
        self.assertFalse(session.transaction_logged)
        self.assertTrue(len(pool._borrowed_sessions), 1)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

        # Fetch new session which will trigger the cleanup task.
        pool.get()
        self.assertTrue(pool._cleanup_task_ongoing_event.is_set())

        pool.stopCleaningLongRunningSessions()
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def test_get_non_empty_session_exists(self):
        pool = self._make_one()
        database = _Database("name")
        previous = _Session(database)
        pool.bind(database)
        pool.put(previous)

        session = pool.get()

        self.assertIs(session, previous)
        session.create.assert_not_called()
        self.assertTrue(session._exists_checked)
        self.assertTrue(pool._sessions.empty())

    def test_get_non_empty_session_expired(self):
        pool = self._make_one()
        database = _Database("name")
        previous = _Session(database, exists=False)
        newborn = _Session(database)
        database._sessions.append(newborn)
        pool.bind(database)
        pool.put(previous)

        session = pool.get()

        self.assertTrue(previous._exists_checked)
        self.assertIs(session, newborn)
        session.create.assert_called()
        self.assertFalse(session._exists_checked)
        self.assertTrue(pool._sessions.empty())

    def test_put_empty(self):
        pool = self._make_one()
        database = _Database("name")
        pool.bind(database)
        session = _Session(database)
        pool._borrowed_sessions.append(session)
        pool._cleanup_task_ongoing_event.set()

        pool.put(session)

        self.assertEqual(len(pool._borrowed_sessions), 0)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())
        self.assertFalse(pool._sessions.empty())

    def test_put_full(self):
        pool = self._make_one(target_size=1)
        database = _Database("name")
        pool.bind(database)
        older = _Session(database)
        pool.put(older)
        self.assertFalse(pool._sessions.empty())

        younger = _Session(database)
        pool.put(younger)  # discarded silently

        self.assertTrue(younger._deleted)
        self.assertIs(pool.get(), older)

    def test_clear(self):
        pool = self._make_one()
        database = _Database("name")
        pool.bind(database)
        previous = _Session(database)
        pool.put(previous)

        pool.clear()

        self.assertTrue(previous._deleted)


class TestPingingPool(unittest.TestCase):
    def _getTargetClass(self):
        from google.cloud.spanner_v1.pool import PingingPool

        return PingingPool

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_ctor_defaults(self):
        pool = self._make_one()
        self.assertIsNone(pool._database)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.default_timeout, 10)
        self.assertEqual(pool._delta.seconds, 3000)
        self.assertTrue(pool._sessions.empty())
        self.assertEqual(pool.labels, {})
        self.assertIsNone(pool.database_role)

    def test_ctor_explicit(self):
        labels = {"foo": "bar"}
        database_role = "dummy-role"
        pool = self._make_one(
            size=4,
            default_timeout=30,
            ping_interval=1800,
            labels=labels,
            database_role=database_role,
        )
        self.assertIsNone(pool._database)
        self.assertEqual(pool.size, 4)
        self.assertEqual(pool.default_timeout, 30)
        self.assertEqual(pool._delta.seconds, 1800)
        self.assertTrue(pool._sessions.empty())
        self.assertEqual(pool.labels, labels)
        self.assertEqual(pool.database_role, database_role)

    def test_ctor_explicit_w_database_role_in_db(self):
        database_role = "dummy-role"
        pool = self._make_one()
        database = pool._database = _Database("name")
        SESSIONS = [_Session(database)] * 10
        database._sessions.extend(SESSIONS)
        database._database_role = database_role
        pool.bind(database)
        self.assertEqual(pool.database_role, database_role)

    def test_bind(self):
        pool = self._make_one()
        database = _Database("name")
        SESSIONS = [_Session(database)] * 10
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        self.assertIs(pool._database, database)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.default_timeout, 10)
        self.assertEqual(pool._delta.seconds, 3000)
        self.assertTrue(pool._sessions.full())

        api = database.spanner_api
        self.assertEqual(api.batch_create_sessions.call_count, 5)
        for session in SESSIONS:
            session.create.assert_not_called()

    def test_get_hit_no_ping(self):
        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 4
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        session = pool.get()

        self.assertIs(session, SESSIONS[0])
        self.assertFalse(session._exists_checked)
        self.assertFalse(pool._sessions.full())

    def test_get_trigger_longrunning_and_set_defaults(self):
        pool = self._make_one(size=2)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 3
        database._sessions.extend(SESSIONS)
        pool.bind(database)
        session = pool.get()
        self.assertIsInstance(session.checkout_time, datetime.datetime)
        self.assertFalse(session.long_running)
        self.assertFalse(session.transaction_logged)
        self.assertTrue(len(pool._borrowed_sessions), 1)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

        # Fetch new session which will trigger the cleanup task.
        pool.get()
        self.assertTrue(pool._cleanup_task_ongoing_event.is_set())

        # Stop the background task.
        pool.stopCleaningLongRunningSessions()
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

    def test_get_hit_w_ping(self):
        import datetime
        from google.cloud._testing import _Monkey
        from google.cloud.spanner_v1 import pool as MUT

        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 4
        database._sessions.extend(SESSIONS)

        sessions_created = datetime.datetime.utcnow() - datetime.timedelta(seconds=4000)

        with _Monkey(MUT, _NOW=lambda: sessions_created):
            pool.bind(database)

        session = pool.get()

        self.assertIs(session, SESSIONS[0])
        self.assertTrue(session._exists_checked)
        self.assertFalse(pool._sessions.full())

    def test_get_hit_w_ping_expired(self):
        import datetime
        from google.cloud._testing import _Monkey
        from google.cloud.spanner_v1 import pool as MUT

        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 5
        SESSIONS[0]._exists = False
        database._sessions.extend(SESSIONS)

        sessions_created = datetime.datetime.utcnow() - datetime.timedelta(seconds=4000)

        with _Monkey(MUT, _NOW=lambda: sessions_created):
            pool.bind(database)

        session = pool.get()

        self.assertIs(session, SESSIONS[4])
        session.create.assert_called()
        self.assertTrue(SESSIONS[0]._exists_checked)
        self.assertFalse(pool._sessions.full())

    def test_get_empty_default_timeout(self):
        import queue

        pool = self._make_one(size=1)
        session_queue = pool._sessions = _Queue()

        with self.assertRaises(queue.Empty):
            pool.get()

        self.assertEqual(session_queue._got, {"block": True, "timeout": 10})

    def test_get_empty_explicit_timeout(self):
        import queue

        pool = self._make_one(size=1, default_timeout=0.1)
        session_queue = pool._sessions = _Queue()

        with self.assertRaises(queue.Empty):
            pool.get(timeout=1)

        self.assertEqual(session_queue._got, {"block": True, "timeout": 1})

    def test_put_full(self):
        import queue

        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 4
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        with self.assertRaises(queue.Full):
            pool.put(_Session(database))

        self.assertTrue(pool._sessions.full())

    def test_put_non_full(self):
        import datetime
        from google.cloud._testing import _Monkey
        from google.cloud.spanner_v1 import pool as MUT

        pool = self._make_one(size=1)
        session_queue = pool._sessions = _Queue()

        now = datetime.datetime.utcnow()
        database = _Database("name")
        session = _Session(database)
        pool._borrowed_sessions.append(session)
        pool._database = database
        pool._cleanup_task_ongoing_event.set()
        with _Monkey(MUT, _NOW=lambda: now):
            pool.put(session)

        self.assertEqual(len(pool._borrowed_sessions), 0)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

        self.assertEqual(len(session_queue._items), 1)
        ping_after, queued = session_queue._items[0]
        self.assertEqual(ping_after, now + datetime.timedelta(seconds=3000))
        self.assertIs(queued, session)

    def test_clear(self):
        pool = self._make_one()
        database = _Database("name")
        SESSIONS = [_Session(database)] * 10
        database._sessions.extend(SESSIONS)
        pool.bind(database)
        self.assertTrue(pool._sessions.full())

        api = database.spanner_api
        self.assertEqual(api.batch_create_sessions.call_count, 5)
        for session in SESSIONS:
            session.create.assert_not_called()

        pool.clear()

        for session in SESSIONS:
            self.assertTrue(session._deleted)

    def test_ping_empty(self):
        pool = self._make_one(size=1)
        pool.ping()  # Does not raise 'Empty'

    def test_ping_oldest_fresh(self):
        pool = self._make_one(size=1)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 1
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        pool.ping()

        self.assertFalse(SESSIONS[0]._pinged)

    def test_ping_oldest_stale_but_exists(self):
        import datetime
        from google.cloud._testing import _Monkey
        from google.cloud.spanner_v1 import pool as MUT

        pool = self._make_one(size=1)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 1
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        later = datetime.datetime.utcnow() + datetime.timedelta(seconds=4000)
        with _Monkey(MUT, _NOW=lambda: later):
            pool.ping()

        self.assertTrue(SESSIONS[0]._pinged)

    def test_ping_oldest_stale_and_not_exists(self):
        import datetime
        from google.cloud._testing import _Monkey
        from google.cloud.spanner_v1 import pool as MUT

        pool = self._make_one(size=1)
        database = _Database("name")
        SESSIONS = [_Session(database)] * 2
        SESSIONS[0]._exists = False
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        later = datetime.datetime.utcnow() + datetime.timedelta(seconds=4000)
        with _Monkey(MUT, _NOW=lambda: later):
            pool.ping()

        self.assertTrue(SESSIONS[0]._pinged)
        SESSIONS[1].create.assert_called()


class TestTransactionPingingPool(unittest.TestCase):
    def _getTargetClass(self):
        from google.cloud.spanner_v1.pool import TransactionPingingPool

        return TransactionPingingPool

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_ctor_defaults(self):
        pool = self._make_one()
        self.assertIsNone(pool._database)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.default_timeout, 10)
        self.assertEqual(pool._delta.seconds, 3000)
        self.assertTrue(pool._sessions.empty())
        self.assertTrue(pool._pending_sessions.empty())
        self.assertEqual(pool.labels, {})
        self.assertIsNone(pool.database_role)

    def test_ctor_explicit(self):
        labels = {"foo": "bar"}
        database_role = "dummy-role"
        pool = self._make_one(
            size=4,
            default_timeout=30,
            ping_interval=1800,
            labels=labels,
            database_role=database_role,
        )
        self.assertIsNone(pool._database)
        self.assertEqual(pool.size, 4)
        self.assertEqual(pool.default_timeout, 30)
        self.assertEqual(pool._delta.seconds, 1800)
        self.assertTrue(pool._sessions.empty())
        self.assertTrue(pool._pending_sessions.empty())
        self.assertEqual(pool.labels, labels)
        self.assertEqual(pool.database_role, database_role)

    def test_ctor_explicit_w_database_role_in_db(self):
        database_role = "dummy-role"
        pool = self._make_one()
        database = pool._database = _Database("name")
        SESSIONS = [_Session(database)] * 10
        database._sessions.extend(SESSIONS)
        database._database_role = database_role
        pool.bind(database)
        self.assertEqual(pool.database_role, database_role)

    def test_bind(self):
        pool = self._make_one()
        database = _Database("name")
        SESSIONS = [_Session(database) for _ in range(10)]
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        self.assertIs(pool._database, database)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.default_timeout, 10)
        self.assertEqual(pool._delta.seconds, 3000)
        self.assertTrue(pool._sessions.full())

        api = database.spanner_api
        self.assertEqual(api.batch_create_sessions.call_count, 5)
        for session in SESSIONS:
            session.create.assert_not_called()
            txn = session._transaction
            txn.begin.assert_not_called()

        self.assertTrue(pool._pending_sessions.empty())

    def test_bind_w_timestamp_race(self):
        import datetime
        from google.cloud._testing import _Monkey
        from google.cloud.spanner_v1 import pool as MUT

        NOW = datetime.datetime.utcnow()
        pool = self._make_one()
        database = _Database("name")
        SESSIONS = [_Session(database) for _ in range(10)]
        database._sessions.extend(SESSIONS)

        with _Monkey(MUT, _NOW=lambda: NOW):
            pool.bind(database)

        self.assertIs(pool._database, database)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.default_timeout, 10)
        self.assertEqual(pool._delta.seconds, 3000)
        self.assertTrue(pool._sessions.full())

        api = database.spanner_api
        self.assertEqual(api.batch_create_sessions.call_count, 5)
        for session in SESSIONS:
            session.create.assert_not_called()
            txn = session._transaction
            txn.begin.assert_not_called()

        self.assertTrue(pool._pending_sessions.empty())

    def test_put_full(self):
        import queue

        pool = self._make_one(size=4)
        database = _Database("name")
        SESSIONS = [_Session(database) for _ in range(4)]
        database._sessions.extend(SESSIONS)
        pool.bind(database)

        with self.assertRaises(queue.Full):
            pool.put(_Session(database))

        self.assertTrue(pool._sessions.full())

    def test_put_non_full_w_active_txn(self):
        pool = self._make_one(size=1)
        session_queue = pool._sessions = _Queue()
        pending = pool._pending_sessions = _Queue()
        database = _Database("name")
        session = _Session(database)
        txn = session.transaction()
        pool._borrowed_sessions.append(session)
        pool._cleanup_task_ongoing_event.set()
        pool._database = database
        pool.put(session)

        self.assertEqual(len(session_queue._items), 1)
        _, queued = session_queue._items[0]
        self.assertIs(queued, session)

        self.assertEqual(len(pending._items), 0)
        self.assertEqual(len(pool._borrowed_sessions), 0)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

        txn.begin.assert_not_called()

    def test_put_non_full_w_committed_txn(self):
        pool = self._make_one(size=1)
        session_queue = pool._sessions = _Queue()
        pending = pool._pending_sessions = _Queue()

        database = _Database("name")
        session = _Session(database)
        database._sessions.extend([session])
        committed = session.transaction()
        committed.committed = True
        pool._borrowed_sessions.append(session)
        pool._cleanup_task_ongoing_event.set()
        pool._database = database
        pool.put(session)

        self.assertEqual(len(session_queue._items), 0)

        self.assertEqual(len(pending._items), 1)
        self.assertIs(pending._items[0], session)
        self.assertIsNot(session._transaction, committed)
        self.assertEqual(len(pool._borrowed_sessions), 0)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

        session._transaction.begin.assert_not_called()

    def test_put_non_full(self):
        pool = self._make_one(size=1)
        session_queue = pool._sessions = _Queue()
        pending = pool._pending_sessions = _Queue()
        database = _Database("name")
        session = _Session(database)
        pool._database = database
        pool._borrowed_sessions.append(session)
        pool._cleanup_task_ongoing_event.set()
        pool.put(session)

        self.assertEqual(len(session_queue._items), 0)
        self.assertEqual(len(pending._items), 1)
        self.assertIs(pending._items[0], session)
        self.assertEqual(len(pool._borrowed_sessions), 0)
        self.assertFalse(pool._cleanup_task_ongoing_event.is_set())

        self.assertFalse(pending.empty())

    def test_begin_pending_transactions_empty(self):
        pool = self._make_one(size=1)
        pool.begin_pending_transactions()  # no raise

    def test_begin_pending_transactions_non_empty(self):
        pool = self._make_one(size=1)
        pool._sessions = _Queue()

        database = _Database("name")
        TRANSACTIONS = [_make_transaction(object())]
        PENDING_SESSIONS = [_Session(database, transaction=txn) for txn in TRANSACTIONS]

        pending = pool._pending_sessions = _Queue(*PENDING_SESSIONS)
        self.assertFalse(pending.empty())
        pool._database = database
        pool.begin_pending_transactions()  # no raise

        for txn in TRANSACTIONS:
            txn.begin.assert_not_called()

        self.assertTrue(pending.empty())


class TestSessionCheckout(unittest.TestCase):
    def _getTargetClass(self):
        from google.cloud.spanner_v1.pool import SessionCheckout

        return SessionCheckout

    def _make_one(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_ctor_wo_kwargs(self):
        pool = _Pool()
        checkout = self._make_one(pool)
        self.assertIs(checkout._pool, pool)
        self.assertIsNone(checkout._session)
        self.assertEqual(checkout._kwargs, {})

    def test_ctor_w_kwargs(self):
        pool = _Pool()
        checkout = self._make_one(pool, foo="bar", database_role="dummy-role")
        self.assertIs(checkout._pool, pool)
        self.assertIsNone(checkout._session)
        self.assertEqual(
            checkout._kwargs, {"foo": "bar", "database_role": "dummy-role"}
        )

    def test_context_manager_wo_kwargs(self):
        database = _Database("name")
        session = _Session(database)
        pool = _Pool(session)
        checkout = self._make_one(pool)

        self.assertEqual(len(pool._items), 1)
        self.assertIs(pool._items[0], session)

        with checkout as borrowed:
            self.assertIs(borrowed, session)
            self.assertEqual(len(pool._items), 0)

        self.assertEqual(len(pool._items), 1)
        self.assertIs(pool._items[0], session)
        self.assertEqual(pool._got, {})

    def test_context_manager_w_kwargs(self):
        database = _Database("name")
        session = _Session(database)
        pool = _Pool(session)
        checkout = self._make_one(pool, foo="bar")

        self.assertEqual(len(pool._items), 1)
        self.assertIs(pool._items[0], session)

        with checkout as borrowed:
            self.assertIs(borrowed, session)
            self.assertEqual(len(pool._items), 0)

        self.assertEqual(len(pool._items), 1)
        self.assertIs(pool._items[0], session)
        self.assertEqual(pool._got, {"foo": "bar"})


def _make_transaction(*args, **kw):
    from google.cloud.spanner_v1.transaction import Transaction

    txn = mock.create_autospec(Transaction)(*args, **kw)
    txn.committed = None
    txn.rolled_back = False
    return txn


@total_ordering
class _Session(object):

    _transaction = None

    def __init__(self, database, exists=True, transaction=None):
        self._database = database
        self._exists = exists
        self._exists_checked = False
        self._pinged = False
        self.create = mock.Mock()
        self._deleted = False
        self._transaction = transaction
        self._session_id = "".join(random.choices(string.ascii_letters, k=10))

    def __lt__(self, other):
        return id(self) < id(other)

    def exists(self):
        self._exists_checked = True
        return self._exists

    def ping(self):
        from google.cloud.exceptions import NotFound

        self._pinged = True
        if not self._exists:
            raise NotFound("expired session")

    def delete(self):
        from google.cloud.exceptions import NotFound

        self._deleted = True
        if not self._exists:
            raise NotFound("unknown session")

    def transaction(self):
        txn = self._transaction = _make_transaction(self)
        return txn


class _Database(object):
    def __init__(self, name):
        self.name = name
        self._sessions = []
        self._database_role = None
        self.database_id = name
        self._route_to_leader_enabled = True
        self.close_inactive_transactions = True
        self.logging_enabled = True
        self._logger = mock.MagicMock()
        self._logger.info = mock.MagicMock()
        self._logger.warning = mock.MagicMock()

        def mock_batch_create_sessions(
            request=None,
            timeout=10,
            metadata=[],
            labels={},
        ):
            from google.cloud.spanner_v1 import BatchCreateSessionsResponse
            from google.cloud.spanner_v1 import Session

            database_role = request.session_template.creator_role if request else None
            if request.session_count < 2:
                response = BatchCreateSessionsResponse(
                    session=[Session(creator_role=database_role, labels=labels)]
                )
            else:
                response = BatchCreateSessionsResponse(
                    session=[
                        Session(creator_role=database_role, labels=labels),
                        Session(creator_role=database_role, labels=labels),
                    ]
                )
            return response

        from google.cloud.spanner_v1 import SpannerClient

        self.spanner_api = mock.create_autospec(SpannerClient, instance=True)
        self.spanner_api.batch_create_sessions.side_effect = mock_batch_create_sessions

    @property
    def database_role(self):
        """Database role used in sessions to connect to this database.

        :rtype: str
        :returns: an str with the name of the database role.
        """
        return self._database_role

    @property
    def logger(self):
        return self._logger

    def session(self, **kwargs):
        # always return first session in the list
        # to avoid reversing the order of putting
        # sessions into pool (important for order tests)
        return self._sessions.pop(0)


class _Queue(object):

    _size = 1

    def __init__(self, *items):
        self._items = list(items)

    def empty(self):
        return len(self._items) == 0

    def full(self):
        return len(self._items) >= self._size

    def get(self, **kwargs):
        import queue

        self._got = kwargs
        try:
            return self._items.pop()
        except IndexError:
            raise queue.Empty()

    def put(self, item, **kwargs):
        self._put = kwargs
        self._items.append(item)

    def put_nowait(self, item, **kwargs):
        self._put_nowait = kwargs
        self._items.append(item)


class _Pool(_Queue):

    _database = None
