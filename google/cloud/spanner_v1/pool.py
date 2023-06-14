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

"""Pools managing shared Session objects."""

import datetime
import queue
import threading
import random
import time
from google.cloud.exceptions import NotFound
from google.cloud.spanner_v1 import BatchCreateSessionsRequest
from google.cloud.spanner_v1 import Session
from google.cloud.spanner_v1._helpers import (
    _metadata_with_prefix,
    _metadata_with_leader_aware_routing,
    DELETE_LONG_RUNNING_TRANSACTION_INTERVAL_SEC,
    DELETE_LONG_RUNNING_TRANSACTION_TIMEOUT_SEC
)
from warnings import warn
import logging

_NOW = datetime.datetime.utcnow  # unit tests may replace

class AbstractSessionPool(object):
    """Specifies required API for concrete session pool implementations.

    :type labels: dict (str -> str) or None
    :param labels: (Optional) user-assigned labels for sessions created
                    by the pool.

    :type database_role: str
    :param database_role: (Optional) user-assigned database_role for the session.
    """

    _database = None
    _sessions = None
    _borrowed_sessions = []
    
    _cleanup_task_ongoing_event = threading.Event()
    _cleanup_task_ongoing = False

    def __init__(self, labels=None, database_role=None):
        if labels is None:
            labels = {}
        self._labels = labels
        self._database_role = database_role

    @property
    def labels(self):
        """User-assigned labels for sessions created by the pool.

        :rtype: dict (str -> str)
        :returns: labels assigned by the user
        """
        return self._labels

    @property
    def database_role(self):
        """User-assigned database_role for sessions created by the pool.

        :rtype: str
        :returns: database_role assigned by the user
        """
        return self._database_role

    def bind(self, database):
        """Associate the pool with a database.

        :type database: :class:`~google.cloud.spanner_v1.database.Database`
        :param database: database used by the pool to create sessions
                         when needed.

        Concrete implementations of this method may pre-fill the pool
        using the database.

        :raises NotImplementedError: abstract method
        """
        raise NotImplementedError()

    def get(self, isLongRunning=False):
        """Check a session out from the pool.

        :type isLongRunning: bool
        :param isLongRunning: Specifies if the session fetched is for long running transaction or not. 

        Concrete implementations of this method are allowed to raise an
        error to signal that the pool is exhausted, or to block until a
        session is available.

        :raises NotImplementedError: abstract method
        """
        raise NotImplementedError()

    def put(self, session):
        """Return a session to the pool.

        :type session: :class:`~google.cloud.spanner_v1.session.Session`
        :param session: the session being returned.

        Concrete implementations of this method are allowed to raise an
        error to signal that the pool is full, or to block until it is
        not full.

        :raises NotImplementedError: abstract method
        """
        raise NotImplementedError()

    def clear(self):
        """Delete all sessions in the pool.

        Concrete implementations of this method are allowed to raise an
        error to signal that the pool is full, or to block until it is
        not full.

        :raises NotImplementedError: abstract method
        """
        raise NotImplementedError()

    def _new_session(self):
        """Helper for concrete methods creating session instances.

        :rtype: :class:`~google.cloud.spanner_v1.session.Session`
        :returns: new session instance.
        """
        return self._database.session(
            labels=self.labels, database_role=self.database_role
        )

    def session(self, **kwargs):
        """Check out a session from the pool.

        :param kwargs: (optional) keyword arguments, passed through to
                       the returned checkout.

        :rtype: :class:`~google.cloud.spanner_v1.session.SessionCheckout`
        :returns: a checkout instance, to be used as a context manager for
                  accessing the session and returning it to the pool.
        """
        return SessionCheckout(self, **kwargs)

    def startCleaningLongRunningSessions(self):
        if not AbstractSessionPool._cleanup_task_ongoing_event.is_set() and not AbstractSessionPool._cleanup_task_ongoing:
            AbstractSessionPool._cleanup_task_ongoing_event.set()
            background = threading.Thread(target=self.deleteLongRunningTransactions, args=(DELETE_LONG_RUNNING_TRANSACTION_INTERVAL_SEC, DELETE_LONG_RUNNING_TRANSACTION_TIMEOUT_SEC,), daemon=True, name='recycle-sessions')
            background.start()

    def stopCleaningLongRunningSessions(self):
        AbstractSessionPool._cleanup_task_ongoing_event.clear()
        AbstractSessionPool._cleanup_task_ongoing = False

    def deleteLongRunningTransactions(self, interval_sec, timeout_sec):
        long_running_transaction_timer = time.time()
        transactions_closed = 0
        while AbstractSessionPool._cleanup_task_ongoing_event.is_set():
            if (time.time() - long_running_transaction_timer >= timeout_sec) and transactions_closed == 0:
                break
            AbstractSessionPool._cleanup_task_ongoing = True
            start = time.time()
            sessions_to_delete = (session for session in self._borrowed_sessions
                      if (datetime.datetime.utcnow() - session.checkout_time > datetime.timedelta(minutes=60))
                      and not session.long_running)
            for session in sessions_to_delete:
                if self._database.close_inactive_transactions:
                    if self._database.logging_enabled:
                        self._database.logger.warn('Long running transaction! Transaction has been closed as it was running for ' +
              'more than 60 minutes. For long running transactions use batch or partitioned transactions.')
                    if session._transaction is not None:
                        session._transaction._session = None
                    if session._batch is not None:
                        session._batch._session = None
                    if session._snapshot is not None:
                        session._snapshpt._session = None
                    transactions_closed += 1
                    self.put(session)
                elif self._database.logging_enabled:
                    if not session.transaction_logged:
                        self._database.logger.warn('Transaction has been running for longer than 60 minutes and might be causing a leak. ' +
                  'Enable closeInactiveTransactions in Session Pool Options to automatically clean such transactions.')
                        session.transaction_logged = True 
            
            elapsed_time = time.time() - start
            if interval_sec - elapsed_time > 0:
                time.sleep(interval_sec - elapsed_time)
        AbstractSessionPool._cleanup_task_ongoing = False
        AbstractSessionPool._cleanup_task_ongoing_event.clear()

class FixedSizePool(AbstractSessionPool):
    """Concrete session pool implementation:

    - Pre-allocates / creates a fixed number of sessions.

    - "Pings" existing sessions via :meth:`session.exists` before returning
      them, and replaces expired sessions.

    - Blocks, with a timeout, when :meth:`get` is called on an empty pool.
      Raises after timing out.

    - Raises when :meth:`put` is called on a full pool.  That error is
      never expected in normal practice, as users should be calling
      :meth:`get` followed by :meth:`put` whenever in need of a session.

    :type size: int
    :param size: fixed pool size

    :type default_timeout: int
    :param default_timeout: default timeout, in seconds, to wait for
                                 a returned session.

    :type labels: dict (str -> str) or None
    :param labels: (Optional) user-assigned labels for sessions created
                    by the pool.

    :type database_role: str
    :param database_role: (Optional) user-assigned database_role for the session.
    """

    DEFAULT_SIZE = 10
    DEFAULT_TIMEOUT = 10

    def __init__(
        self,
        size=DEFAULT_SIZE,
        default_timeout=DEFAULT_TIMEOUT,
        labels=None,
        database_role=None,
    ):
        super(FixedSizePool, self).__init__(labels=labels, database_role=database_role)
        self.size = size
        self.default_timeout = default_timeout
        self._sessions = queue.LifoQueue(size)
        self._borrowed_sessions = []
    def bind(self, database):
        """Associate the pool with a database.

        :type database: :class:`~google.cloud.spanner_v1.database.Database`
        :param database: database used by the pool to used to create sessions
                         when needed.
        """
        self._database = database
        api = database.spanner_api
        metadata = _metadata_with_prefix(database.name)
        if database._route_to_leader_enabled:
            metadata.append(
                _metadata_with_leader_aware_routing(database._route_to_leader_enabled)
            )
        self._database_role = self._database_role or self._database.database_role
        request = BatchCreateSessionsRequest(
            database=database.name,
            session_count=self.size - self._sessions.qsize(),
            session_template=Session(creator_role=self.database_role),
        )

        while not self._sessions.full():
            resp = api.batch_create_sessions(
                request=request,
                metadata=metadata,
            )
            for session_pb in resp.session:
                session = self._new_session()
                session._session_id = session_pb.name.split("/")[-1]
                self._sessions.put(session)

    def get(self, isLongRunning=False, timeout=None):
        """Check a session out from the pool.

        :type timeout: int
        :param timeout: seconds to block waiting for an available session

        :rtype: :class:`~google.cloud.spanner_v1.session.Session`
        :returns: an existing session from the pool, or a newly-created
                  session.
        :raises: :exc:`queue.Empty` if the queue is empty.
        """
        if timeout is None:
            timeout = self.default_timeout

        session = self._sessions.get(block=True, timeout=timeout)

        if not session.exists():
            session = self._database.session()
            session.create()

        session.checkout_time = datetime.datetime.utcnow()
        session.long_running = isLongRunning
        session.transaction_logged = False
        self._borrowed_sessions.append(session)
        if (self._database.close_inactive_transactions or self._database.logging_enabled) and len(self._borrowed_sessions)/self.size >= 0.95 :
            self.startCleaningLongRunningSessions()
        return session

    def put(self, session):
        """Return a session to the pool.

        Never blocks:  if the pool is full, raises.

        :type session: :class:`~google.cloud.spanner_v1.session.Session`
        :param session: the session being returned.

        :raises: :exc:`queue.Full` if the queue is full.
        """
        if self._borrowed_sessions.__contains__(session):
                self._borrowed_sessions.remove(session)
        self._sessions.put_nowait(session)
        
        if (self._database.close_inactive_transactions or self._database.logging_enabled) and len(self._borrowed_sessions)/self.size < 0.95 :
            self.stopCleaningLongRunningSessions()

    def clear(self):
        """Delete all sessions in the pool."""

        while True:
            try:
                session = self._sessions.get(block=False)
            except queue.Empty:
                break
            else:
                session.delete()


class BurstyPool(AbstractSessionPool):
    """Concrete session pool implementation:

    - "Pings" existing sessions via :meth:`session.exists` before returning
      them.

    - Creates a new session, rather than blocking, when :meth:`get` is called
      on an empty pool.

    - Discards the returned session, rather than blocking, when :meth:`put`
      is called on a full pool.

    :type target_size: int
    :param target_size: max pool size

    :type labels: dict (str -> str) or None
    :param labels: (Optional) user-assigned labels for sessions created
                    by the pool.

    :type database_role: str
    :param database_role: (Optional) user-assigned database_role for the session.
    """

    def __init__(self, target_size=10, labels=None, database_role=None):
        super(BurstyPool, self).__init__(labels=labels, database_role=database_role)
        self.target_size = target_size
        self._database = None
        self._sessions = queue.LifoQueue(target_size)
        self._borrowed_sessions = []

    def bind(self, database):
        """Associate the pool with a database.

        :type database: :class:`~google.cloud.spanner_v1.database.Database`
        :param database: database used by the pool to create sessions
                         when needed.
        """
        self._database = database
        self._database_role = self._database_role or self._database.database_role

    def get(self, isLongRunning=False):
        """Check a session out from the pool.

        :rtype: :class:`~google.cloud.spanner_v1.session.Session`
        :returns: an existing session from the pool, or a newly-created
                  session.
        """
        try:
            session = self._sessions.get_nowait()
        except queue.Empty:
            session = self._new_session()
            session.create()
        else:
            if not session.exists():
                session = self._new_session()
                session.create()
        session.checkout_time = datetime.datetime.utcnow()
        session.long_running = isLongRunning
        session.transaction_logged = False
        self._borrowed_sessions.append(session)
        if (self._database.close_inactive_transactions or self._database.logging_enabled) and len(self._borrowed_sessions)/self.target_size >= 0.95 :
            self.startCleaningLongRunningSessions()
        return session

    def put(self, session):
        """Return a session to the pool.

        Never blocks:  if the pool is full, the returned session is
        discarded.

        :type session: :class:`~google.cloud.spanner_v1.session.Session`
        :param session: the session being returned.
        """
        try:
            if self._borrowed_sessions.__contains__(session):
                self._borrowed_sessions.remove(session)
            self._sessions.put_nowait(session)
            if (self._database.close_inactive_transactions or self._database.logging_enabled) and len(self._borrowed_sessions)/self.target_size < 0.95 :
                self.stopCleaningLongRunningSessions()
        except queue.Full:
            try:
                session.delete()
            except NotFound:
                pass

    def clear(self):
        """Delete all sessions in the pool."""

        while True:
            try:
                session = self._sessions.get(block=False)
            except queue.Empty:
                break
            else:
                session.delete()


class PingingPool(AbstractSessionPool):
    """Concrete session pool implementation:

    - Pre-allocates / creates a fixed number of sessions.

    - Sessions are used in "round-robin" order (LRU first).

    - "Pings" existing sessions in the background after a specified interval
      via an API call (``session.ping()``).

    - Blocks, with a timeout, when :meth:`get` is called on an empty pool.
      Raises after timing out.

    - Raises when :meth:`put` is called on a full pool.  That error is
      never expected in normal practice, as users should be calling
      :meth:`get` followed by :meth:`put` whenever in need of a session.

    The application is responsible for calling :meth:`ping` at appropriate
    times, e.g. from a background thread.

    :type size: int
    :param size: fixed pool size

    :type default_timeout: int
    :param default_timeout: default timeout, in seconds, to wait for
                            a returned session.

    :type ping_interval: int
    :param ping_interval: interval at which to ping sessions.

    :type labels: dict (str -> str) or None
    :param labels: (Optional) user-assigned labels for sessions created
                    by the pool.

    :type database_role: str
    :param database_role: (Optional) user-assigned database_role for the session.
    """

    def __init__(
        self,
        size=10,
        default_timeout=10,
        ping_interval=3000,
        labels=None,
        database_role=None,
    ):
        super(PingingPool, self).__init__(labels=labels, database_role=database_role)
        self.size = size
        self.default_timeout = default_timeout
        self._delta = datetime.timedelta(seconds=ping_interval)
        self._sessions = queue.PriorityQueue(size)
        self._borrowed_sessions = []

    def bind(self, database):
        """Associate the pool with a database.

        :type database: :class:`~google.cloud.spanner_v1.database.Database`
        :param database: database used by the pool to create sessions
                         when needed.
        """
        self._database = database
        api = database.spanner_api
        metadata = _metadata_with_prefix(database.name)
        if database._route_to_leader_enabled:
            metadata.append(
                _metadata_with_leader_aware_routing(database._route_to_leader_enabled)
            )
        created_session_count = 0
        self._database_role = self._database_role or self._database.database_role

        request = BatchCreateSessionsRequest(
            database=database.name,
            session_count=self.size - created_session_count,
            session_template=Session(creator_role=self.database_role),
        )

        while created_session_count < self.size:
            resp = api.batch_create_sessions(
                request=request,
                metadata=metadata,
            )
            for session_pb in resp.session:
                session = self._new_session()
                session._session_id = session_pb.name.split("/")[-1]
                self.put(session)
            created_session_count += len(resp.session)

    def get(self, isLongRunning=False, timeout=None):
        """Check a session out from the pool.

        :type timeout: int
        :param timeout: seconds to block waiting for an available session

        :rtype: :class:`~google.cloud.spanner_v1.session.Session`
        :returns: an existing session from the pool, or a newly-created
                  session.
        :raises: :exc:`queue.Empty` if the queue is empty.
        """
        if timeout is None:
            timeout = self.default_timeout

        ping_after, session = self._sessions.get(block=True, timeout=timeout)

        if _NOW() > ping_after:
            # Using session.exists() guarantees the returned session exists.
            # session.ping() uses a cached result in the backend which could
            # result in a recently deleted session being returned.
            if not session.exists():
                session = self._new_session()
                session.create()

        session.checkout_time = datetime.datetime.utcnow()
        session.long_running = isLongRunning
        session.transaction_logged = False
        self._borrowed_sessions.append(session)
        if (self._database.close_inactive_transactions or self._database.logging_enabled) and len(self._borrowed_sessions)/self.size >= 0.95 :
            self.startCleaningLongRunningSessions()
        return session

    def put(self, session):
        """Return a session to the pool.

        Never blocks:  if the pool is full, raises.

        :type session: :class:`~google.cloud.spanner_v1.session.Session`
        :param session: the session being returned.

        :raises: :exc:`queue.Full` if the queue is full.
        """
        if self._borrowed_sessions.__contains__(session):
                self._borrowed_sessions.remove(session)
        self._sessions.put_nowait((_NOW() + self._delta, session))
        if (self._database.close_inactive_transactions or self._database.logging_enabled) and len(self._borrowed_sessions)/self.size < 0.95 :
            self.stopCleaningLongRunningSessions()

    def clear(self):
        """Delete all sessions in the pool."""
        while True:
            try:
                _, session = self._sessions.get(block=False)
            except queue.Empty:
                break
            else:
                session.delete()

    def ping(self):
        """Refresh maybe-expired sessions in the pool.

        This method is designed to be called from a background thread,
        or during the "idle" phase of an event loop.
        """
        while True:
            try:
                ping_after, session = self._sessions.get(block=False)
            except queue.Empty:  # all sessions in use
                break
            if ping_after > _NOW():  # oldest session is fresh
                # Re-add to queue with existing expiration
                self._sessions.put((ping_after, session))
                break
            try:
                session.ping()
            except NotFound:
                session = self._new_session()
                session.create()
            # Re-add to queue with new expiration
            self.put(session)


class TransactionPingingPool(PingingPool):
    """Concrete session pool implementation:

    Deprecated: TransactionPingingPool no longer begins a transaction for each of its sessions at startup.
    Hence the TransactionPingingPool is same as :class:`PingingPool` and maybe removed in the future.


    In addition to the features of :class:`PingingPool`, this class
    creates and begins a transaction for each of its sessions at startup.

    When a session is returned to the pool, if its transaction has been
    committed or rolled back, the pool creates a new transaction for the
    session and pushes the transaction onto a separate queue of "transactions
    to begin."  The application is responsible for flushing this queue
    as appropriate via the pool's :meth:`begin_pending_transactions` method.

    :type size: int
    :param size: fixed pool size

    :type default_timeout: int
    :param default_timeout: default timeout, in seconds, to wait for
                            a returned session.

    :type ping_interval: int
    :param ping_interval: interval at which to ping sessions.

    :type labels: dict (str -> str) or None
    :param labels: (Optional) user-assigned labels for sessions created
                    by the pool.

    :type database_role: str
    :param database_role: (Optional) user-assigned database_role for the session.
    """

    def __init__(
        self,
        size=10,
        default_timeout=10,
        ping_interval=3000,
        labels=None,
        database_role=None,
    ):
        """This throws a deprecation warning on initialization."""
        warn(
            f"{self.__class__.__name__} is deprecated.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._pending_sessions = queue.Queue()
        self._borrowed_sessions = []

        super(TransactionPingingPool, self).__init__(
            size,
            default_timeout,
            ping_interval,
            labels=labels,
            database_role=database_role,
        )

        self.begin_pending_transactions()

    def bind(self, database):
        """Associate the pool with a database.

        :type database: :class:`~google.cloud.spanner_v1.database.Database`
        :param database: database used by the pool to create sessions
                         when needed.
        """
        super(TransactionPingingPool, self).bind(database)
        self._database_role = self._database_role or self._database.database_role
        self.begin_pending_transactions()

    def put(self, session):
        """Return a session to the pool.

        Never blocks:  if the pool is full, raises.

        :type session: :class:`~google.cloud.spanner_v1.session.Session`
        :param session: the session being returned.

        :raises: :exc:`queue.Full` if the queue is full.
        """
        if self._sessions.full():
            raise queue.Full

        txn = session._transaction
        if txn is None or txn.committed or txn.rolled_back:
            session.transaction()
            if self._borrowed_sessions.__contains__(session):
                self._borrowed_sessions.remove(session)
            self._pending_sessions.put(session)
            if (self._database.close_inactive_transactions or self._database.logging_enabled) and len(self._borrowed_sessions)/self.size < 0.95 :
                self.stopCleaningLongRunningSessions()
        else:
            super(TransactionPingingPool, self).put(session)

    def begin_pending_transactions(self):
        """Begin all transactions for sessions added to the pool."""
        while not self._pending_sessions.empty():
            session = self._pending_sessions.get()
            super(TransactionPingingPool, self).put(session)


class SessionCheckout(object):
    """Context manager: hold session checked out from a pool.

    :type pool: concrete subclass of
        :class:`~google.cloud.spanner_v1.pool.AbstractSessionPool`
    :param pool: Pool from which to check out a session.

    :param kwargs: extra keyword arguments to be passed to :meth:`pool.get`.
    """

    _session = None  # Not checked out until '__enter__'.

    def __init__(self, pool, **kwargs):
        self._pool = pool
        self._kwargs = kwargs.copy()

    def __enter__(self):
        self._session = self._pool.get(**self._kwargs)
        return self._session

    def __exit__(self, *ignored):
        if not (self._session._transaction is not None and self._session._transaction._session is None):
            self._pool.put(self._session)
