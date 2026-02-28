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
    TransactionOptions,
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

    def test_read_write_no_begin_transaction_rpc(self):
        """Read-write DBAPI transaction must not send BeginTransactionRequest."""
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            cursor.fetchall()
        connection.commit()

        begin_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, BeginTransactionRequest)
        ]
        self.assertEqual(0, len(begin_requests),
                         "Read-write DBAPI transactions should not send "
                         "a separate BeginTransactionRequest")

    def test_read_write_uses_inline_begin(self):
        """The first ExecuteSqlRequest must carry TransactionSelector(begin=...)."""
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            cursor.fetchall()
        connection.commit()

        sql_requests = [
            r for r in self.spanner_service.requests
            if isinstance(r, ExecuteSqlRequest)
        ]
        self.assertGreaterEqual(len(sql_requests), 1)
        first_sql = sql_requests[0]
        self.assertIn(
            "read_write", first_sql.transaction.begin,
            "First ExecuteSqlRequest should use inline begin with "
            "TransactionSelector(begin=ReadWrite(...))",
        )

    def test_read_write_request_sequence(self):
        """Read-write DBAPI transaction: ExecuteSql + Commit (no BeginTransaction)."""
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            cursor.fetchall()
        connection.commit()

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

    def test_read_then_write_request_sequence(self):
        """Read + write in same transaction: 2x ExecuteSql + Commit."""
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            cursor.fetchall()
            cursor.execute(
                "insert into singers (id, name) values (1, 'Some Singer')"
            )
        connection.commit()

        self.assert_requests_sequence(
            self.spanner_service.requests,
            [ExecuteSqlRequest, ExecuteSqlRequest, CommitRequest],
            TransactionType.READ_WRITE,
        )

    def test_read_only_still_uses_explicit_begin(self):
        """Read-only transactions should still use explicit BeginTransaction."""
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        connection.read_only = True
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            cursor.fetchall()
        connection.commit()

        self.assert_requests_sequence(
            self.spanner_service.requests,
            [BeginTransactionRequest, ExecuteSqlRequest],
            TransactionType.READ_ONLY,
        )

    def test_second_statement_uses_transaction_id(self):
        """After inline begin, subsequent statements must use TransactionSelector(id=...).

        This verifies that the DBAPI correctly extracts the transaction_id from
        the inline begin response and passes it to subsequent requests — proving
        the transaction lifecycle is maintained.
        """
        connection = Connection(self.instance, self.database)
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("select name from singers")
            cursor.fetchall()
            cursor.execute(
                "insert into singers (id, name) values (1, 'Some Singer')"
            )
        connection.commit()

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
            "with the transaction_id returned from inline begin, "
            "not another TransactionSelector(begin=...)",
        )

    def test_rollback(self):
        """Rollback should work without error after inline begin."""
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
        self.assertEqual(0, len(begin_requests))

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
