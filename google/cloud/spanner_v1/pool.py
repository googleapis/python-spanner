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

"""Pools managing shared Session objects.

.. deprecated::
    Session pools are deprecated and will be removed in a future release.
    Multiplexed sessions are now used for all operations by default, eliminating
    the need for session pooling.
"""

from warnings import warn

_POOL_DEPRECATION_MESSAGE = (
    "Session pools are deprecated and will be removed in a future release. "
    "Multiplexed sessions are now used for all operations by default, "
    "eliminating the need for session pooling. "
    "To disable this warning, do not pass a pool argument when creating a Database."
)


class AbstractSessionPool:
    """Specifies required API for concrete session pool implementations.

    .. deprecated::
        Session pools are deprecated and will be removed in a future release.
        Multiplexed sessions are now used for all operations by default.

    :type labels: dict (str -> str) or None
    :param labels: (Optional) user-assigned labels for sessions created
                    by the pool.

    :type database_role: str
    :param database_role: (Optional) user-assigned database_role for the session.
    """

    _database = None

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
        """Associate the pool with a database. No-op for deprecated pools.

        :type database: :class:`~google.cloud.spanner_v1.database.Database`
        :param database: database used by the pool (ignored).
        """
        self._database = database

    def get(self):
        """Check a session out from the pool. No-op for deprecated pools.

        :raises NotImplementedError: pools are deprecated
        """
        raise NotImplementedError("Session pools are deprecated")

    def put(self, session):
        """Return a session to the pool. No-op for deprecated pools.

        :type session: :class:`~google.cloud.spanner_v1.session.Session`
        :param session: the session being returned (ignored).
        """
        pass

    def clear(self):
        """Delete all sessions in the pool. No-op for deprecated pools."""
        pass


class FixedSizePool(AbstractSessionPool):
    """Concrete session pool implementation.

    .. deprecated::
        FixedSizePool is deprecated and will be removed in a future release.
        Multiplexed sessions are now used for all operations by default.
    """

    DEFAULT_SIZE = 10
    DEFAULT_TIMEOUT = 10

    def __init__(
        self,
        size=DEFAULT_SIZE,
        default_timeout=DEFAULT_TIMEOUT,
        labels=None,
        database_role=None,
        max_age_minutes=55,
    ):
        warn(_POOL_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
        super(FixedSizePool, self).__init__(labels=labels, database_role=database_role)
        self.size = size
        self.default_timeout = default_timeout

    def get(self, timeout=None):
        """Check a session out from the pool. No-op for deprecated pools.

        :raises NotImplementedError: pools are deprecated
        """
        raise NotImplementedError("Session pools are deprecated")


class BurstyPool(AbstractSessionPool):
    """Concrete session pool implementation.

    .. deprecated::
        BurstyPool is deprecated and will be removed in a future release.
        Multiplexed sessions are now used for all operations by default.
    """

    def __init__(self, target_size=10, labels=None, database_role=None):
        warn(_POOL_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
        super(BurstyPool, self).__init__(labels=labels, database_role=database_role)
        self.target_size = target_size

    def get(self):
        """Check a session out from the pool. No-op for deprecated pools.

        :raises NotImplementedError: pools are deprecated
        """
        raise NotImplementedError("Session pools are deprecated")


class PingingPool(AbstractSessionPool):
    """Concrete session pool implementation.

    .. deprecated::
        PingingPool is deprecated and will be removed in a future release.
        Multiplexed sessions are now used for all operations by default.
    """

    def __init__(
        self,
        size=10,
        default_timeout=10,
        ping_interval=3000,
        labels=None,
        database_role=None,
    ):
        warn(_POOL_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
        super(PingingPool, self).__init__(labels=labels, database_role=database_role)
        self.size = size
        self.default_timeout = default_timeout

    def get(self, timeout=None):
        """Check a session out from the pool. No-op for deprecated pools.

        :raises NotImplementedError: pools are deprecated
        """
        raise NotImplementedError("Session pools are deprecated")

    def ping(self):
        """Refresh maybe-expired sessions in the pool. No-op for deprecated pools."""
        pass


class TransactionPingingPool(PingingPool):
    """Concrete session pool implementation.

    .. deprecated::
        TransactionPingingPool is deprecated and will be removed in a future release.
        Multiplexed sessions are now used for all operations by default.
    """

    def __init__(
        self,
        size=10,
        default_timeout=10,
        ping_interval=3000,
        labels=None,
        database_role=None,
    ):
        # Call grandparent's __init__ directly to avoid double deprecation warning
        AbstractSessionPool.__init__(self, labels=labels, database_role=database_role)
        warn(_POOL_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
        self.size = size
        self.default_timeout = default_timeout

    def begin_pending_transactions(self):
        """Begin all transactions for sessions added to the pool. No-op for deprecated pools."""
        pass


class SessionCheckout:
    """Context manager: hold session checked out from a pool.

    .. deprecated::
        SessionCheckout is deprecated and will be removed in a future release.
        Multiplexed sessions are now used for all operations by default.
    """

    _session = None

    def __init__(self, pool, **kwargs):
        warn(
            "SessionCheckout is deprecated. "
            "Sessions should be managed through database context managers or "
            "run_in_transaction instead of being checked out directly from the pool.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._pool = pool
        self._kwargs = kwargs.copy()

    def __enter__(self):
        raise NotImplementedError("Session pools are deprecated")

    def __exit__(self, *ignored):
        pass
