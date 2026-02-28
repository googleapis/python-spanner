# Copyright 2026 Google LLC All rights reserved.
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

"""Tests that the DBAPI uses inline begin for read-write transactions.

After removing the explicit ``Transaction.begin()`` call from
``Connection.transaction_checkout()``, the DBAPI should piggyback
``BeginTransaction`` on the first ``ExecuteSql`` / ``ExecuteUpdate`` request
via ``TransactionSelector(begin=...)``, eliminating one gRPC round-trip
per transaction.

Read-only transactions are unaffected — they still use an explicit
``BeginTransaction`` RPC via ``snapshot_checkout()``.
"""

from google.cloud.spanner_dbapi import Connection
from google.cloud.spanner_v1 import (
    BeginTransactionRequest,
    CommitRequest,
    ExecuteSqlRequest,
    RollbackRequest,
    TypeCode,
)
from google.cloud.spanner_v1.testing.mock_spanner import SpannerServicer
from google.cloud.spanner_v1.database_sessions_manager import TransactionType

from tests.mockserver_tests.mock_server_test_base import (
    MockServerTestBase,
    add_single_result,
    add_update_count,
    add_error,
    aborted_status,
)


class TestDbapiInlineBegin(MockServerTestBase):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        add_single_result(
            "select name from singers", "name", TypeCode.STRING, [("Some Singer",)]
        )
        add_update_count(
            "insert into singers (id, name) values (1, 'Some Singer')", 1
        )

    def test_read_write_inline_begin(self):
        """Comprehensive check for a single-statement read-write transaction.

        Verifies:
        - No BeginTransactionRequest is sent
        - The ExecuteSqlRequest uses TransactionSelector(begin=ReadWrite(...))
        - The request sequence is [ExecuteSqlRequest, CommitRequest]
        - The query returns correct data
        """
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            rows = cursor.fetchall()
        connection.commit()

        self.assertEqual(
            [("Some Singer",)], rows,
            "Query should return the mocked result set",
        )

        begin_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, BeginTransactionRequest)
        ]
        self.assertEqual(0, len(begin_requests),
                         "Read-write DBAPI transactions should not send "
                         "a separate BeginTransactionRequest")

        sql_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, ExecuteSqlRequest)
        ]
        self.assertGreaterEqual(len(sql_requests), 1)
        first_sql = sql_requests[0]
        self.assertTrue(
            first_sql.transaction.begin.read_write == first_sql.transaction.begin.read_write,
        )
        self.assertIn(
            "read_write", first_sql.transaction.begin,
            "First ExecuteSqlRequest should use inline begin with "
            "TransactionSelector(begin=ReadWrite(...))",
        )

        self.assert_requests_sequence(
            self.spanner_service.requests,
            [ExecuteSqlRequest, CommitRequest],
            TransactionType.READ_WRITE,
        )

    def test_read_write_dml_request_sequence(self):
        """DML write via DBAPI: ExecuteSql + Commit (no BeginTransaction)."""
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute(
                "insert into singers (id, name) values (1, 'Some Singer')"
            )
        connection.commit()

        self.assert_requests_sequence(
            self.spanner_service.requests,
            [ExecuteSqlRequest, CommitRequest],
            TransactionType.READ_WRITE,
        )

    def test_read_then_write_full_lifecycle(self):
        """Read + write in same transaction: verifies the complete inline begin lifecycle.

        Checks:
        - First ExecuteSqlRequest uses TransactionSelector(begin=ReadWrite(...))
        - Second ExecuteSqlRequest uses TransactionSelector(id=<txn_id>)
        - CommitRequest uses the same transaction_id as the second statement
        - Query returns correct data
        - Request sequence is [ExecuteSql, ExecuteSql, Commit]
        """
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            rows = cursor.fetchall()
            cursor.execute(
                "insert into singers (id, name) values (1, 'Some Singer')"
            )
        connection.commit()

        self.assertEqual(
            [("Some Singer",)], rows,
            "Query should return the mocked result set",
        )

        self.assert_requests_sequence(
            self.spanner_service.requests,
            [ExecuteSqlRequest, ExecuteSqlRequest, CommitRequest],
            TransactionType.READ_WRITE,
        )

        sql_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, ExecuteSqlRequest)
        ]
        self.assertEqual(2, len(sql_requests))

        first = sql_requests[0]
        self.assertIn(
            "read_write", first.transaction.begin,
            "First statement should use inline begin",
        )

        second = sql_requests[1]
        self.assertNotEqual(
            b"", second.transaction.id,
            "Second statement should use TransactionSelector(id=...) "
            "with the transaction_id returned from inline begin",
        )

        commit_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, CommitRequest)
        ]
        self.assertEqual(1, len(commit_requests))
        self.assertEqual(
            second.transaction.id, commit_requests[0].transaction_id,
            "CommitRequest must reference the same transaction_id "
            "that the second ExecuteSqlRequest used",
        )

    def test_read_only_still_uses_explicit_begin(self):
        """Read-only transactions should still use explicit BeginTransaction."""
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        connection.read_only = True
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            rows = cursor.fetchall()
        connection.commit()

        self.assertEqual(
            [("Some Singer",)], rows,
            "Read-only query should return the mocked result set",
        )

        self.assert_requests_sequence(
            self.spanner_service.requests,
            [BeginTransactionRequest, ExecuteSqlRequest],
            TransactionType.READ_ONLY,
        )

    def test_rollback_after_inline_begin(self):
        """Rollback after DML sends RollbackRequest with the correct transaction_id."""
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute(
                "insert into singers (id, name) values (1, 'Some Singer')"
            )
        connection.rollback()

        begin_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, BeginTransactionRequest)
        ]
        self.assertEqual(0, len(begin_requests),
                         "Rollback path should not use BeginTransactionRequest")

        sql_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, ExecuteSqlRequest)
        ]
        self.assertEqual(1, len(sql_requests))

        rollback_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, RollbackRequest)
        ]
        self.assertEqual(1, len(rollback_requests),
                         "A RollbackRequest should be sent after DML + rollback")

        txn_id_from_inline_begin = sql_requests[0].transaction.begin
        self.assertIn(
            "read_write", txn_id_from_inline_begin,
            "DML should have used inline begin",
        )

        self.assertNotEqual(
            b"", rollback_requests[0].transaction_id,
            "RollbackRequest must carry the transaction_id obtained via inline begin",
        )

    def test_inline_begin_with_abort_retry(self):
        """Transaction retry after abort should work with inline begin.

        The DBAPI replays recorded statements on abort. With inline begin,
        the retried ExecuteSqlRequest should again use inline begin.
        """
        add_error(SpannerServicer.Commit.__name__, aborted_status())

        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute(
                "insert into singers (id, name) values (1, 'Some Singer')"
            )
        connection.commit()

        begin_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, BeginTransactionRequest)
        ]
        self.assertEqual(0, len(begin_requests),
                         "Retried transaction should also use inline begin, "
                         "not explicit BeginTransactionRequest")

        sql_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, ExecuteSqlRequest)
        ]
        self.assertEqual(2, len(sql_requests),
                         "Expected 2 ExecuteSqlRequests: original + retry")
        for i, req in enumerate(sql_requests):
            self.assertIn(
                "read_write", req.transaction.begin,
                f"ExecuteSqlRequest[{i}] should use inline begin",
            )

        commit_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, CommitRequest)
        ]
        self.assertEqual(2, len(commit_requests),
                         "Expected 2 CommitRequests: the aborted original + "
                         "the successful retry")
        for i, cr in enumerate(commit_requests):
            self.assertNotEqual(
                b"", cr.transaction_id,
                f"CommitRequest[{i}] must carry a transaction_id "
                "from inline begin",
            )
