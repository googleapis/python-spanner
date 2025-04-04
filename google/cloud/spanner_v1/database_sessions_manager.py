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
from google.cloud.spanner_v1._opentelemetry_tracing import (
    get_current_span,
    add_span_event,
)
from google.cloud.spanner_v1.session import Session


class DatabaseSessionsManager(object):
    """Manages sessions for a Cloud Spanner database.

    Sessions can be checked out from the database session manager using :meth:`get_session_for_read_only`,
    :meth:`get_session_for_partitioned`, and :meth:`get_session_for_read_write`, and returned to
    the session manager using :meth:`put_session`.

    The sessions returned by the session manager depend on the client's session options (see
    :class:`~google.cloud.spanner_v1.session_options.SessionOptions`) and the provided session
    pool (see :class:`~google.cloud.spanner_v1.pool.AbstractSessionPool`).

    :type database: :class:`~google.cloud.spanner_v1.database.Database`
    :param database: The database to manage sessions for.

    :type pool: :class:`~google.cloud.spanner_v1.pool.AbstractSessionPool`
    :param pool: The pool to get non-multiplexed sessions from.
    """

    def __init__(self, database, pool):
        self._database = database
        self._pool = pool

    def get_session_for_read_only(self) -> Session:
        """Returns a session for read-only transactions from the database session manager.

        :rtype: :class:`~google.cloud.spanner_v1.session.Session`
        :returns: a session for read-only transactions.
        """

        if (
            self._database._instance._client.session_options.use_multiplexed_for_read_only()
        ):
            raise NotImplementedError(
                "Multiplexed sessions are not yet supported for read-only transactions."
            )

        return self._get_pooled_session()

    def get_session_for_partitioned(self) -> Session:
        """Returns a session for partitioned transactions from the database session manager.

        :rtype: :class:`~google.cloud.spanner_v1.session.Session`
        :returns: a session for partitioned transactions.
        """

        if (
            self._database._instance._client.session_options.use_multiplexed_for_partitioned()
        ):
            raise NotImplementedError(
                "Multiplexed sessions are not yet supported for partitioned transactions."
            )

        return self._get_pooled_session()

    def get_session_for_read_write(self) -> Session:
        """Returns a session for read/write transactions from the database session manager.

        :rtype: :class:`~google.cloud.spanner_v1.session.Session`
        :returns: a session for read/write transactions.
        """

        if (
            self._database._instance._client.session_options.use_multiplexed_for_read_write()
        ):
            raise NotImplementedError(
                "Multiplexed sessions are not yet supported for read/write transactions."
            )

        return self._get_pooled_session()

    def put_session(self, session: Session) -> None:
        """Returns the session to the database session manager."""

        if session.is_multiplexed:
            raise NotImplementedError("Multiplexed sessions are not yet supported.")

        self._pool.put(session)

        current_span = get_current_span()
        add_span_event(
            current_span,
            "Returned session",
            {"id": session.session_id, "multiplexed": session.is_multiplexed},
        )

    def _get_pooled_session(self):
        """Returns a non-multiplexed session from the session pool."""

        session = self._pool.get()
        add_span_event(
            get_current_span(),
            "Using session",
            {"id": session.session_id, "multiplexed": session.is_multiplexed},
        )

        return session
