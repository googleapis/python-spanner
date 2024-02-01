# Copyright 2023 Google LLC All rights reserved.
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

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, List

from google.cloud.spanner_dbapi import parse_utils
from google.cloud.spanner_dbapi.parsed_statement import (
    ParsedStatement,
    StatementType,
    Statement,
)
from google.rpc.code_pb2 import ABORTED, OK
from google.api_core.exceptions import Aborted

from google.cloud.spanner_dbapi.utils import StreamedManyResultSets

if TYPE_CHECKING:
    from google.cloud.spanner_dbapi.cursor import Cursor
    from google.cloud.spanner_dbapi.connection import Connection


class BatchDdlExecutor:
    """Executor that is used when a DDL batch is started. These batches only
    accept DDL statements. All DDL statements are buffered locally and sent to
    Spanner when runBatch() is called.

    :type "Connection": :class:`~google.cloud.spanner_dbapi.connection.Connection`
    :param connection:
    """

    def __init__(self, connection: "Connection"):
        self._connection = connection
        self._statements: List[str] = []

    def execute_statement(self, parsed_statement: ParsedStatement):
        """Executes the statement when ddl batch is active by buffering the
        statement in-memory.

        :type parsed_statement: ParsedStatement
        :param parsed_statement: parsed statement containing sql query
        """
        from google.cloud.spanner_dbapi import ProgrammingError

        if parsed_statement.statement_type != StatementType.DDL:
            raise ProgrammingError("Only DDL statements are allowed in batch DDL mode.")
        self._statements.extend(
            parse_utils.parse_and_get_ddl_statements(parsed_statement.statement.sql)
        )

    def run_batch(self):
        """Executes all the buffered statements on the active ddl batch by
        making a call to Spanner.
        """
        from google.cloud.spanner_dbapi import ProgrammingError

        if self._connection._client_transaction_started:
            raise ProgrammingError(
                "Cannot execute DDL statement when transaction is already active."
            )
        return self._connection.database.update_ddl(self._statements).result()


class BatchDmlExecutor:
    """Executor that is used when a DML batch is started. These batches only
    accept DML statements. All DML statements are buffered locally and sent to
    Spanner when runBatch() is called.

    :type "Cursor": :class:`~google.cloud.spanner_dbapi.cursor.Cursor`
    :param cursor:
    """

    def __init__(self, cursor: "Cursor"):
        self._cursor = cursor
        self._connection = cursor.connection
        self._statements: List[Statement] = []

    def execute_statement(self, parsed_statement: ParsedStatement):
        """Executes the statement when dml batch is active by buffering the
        statement in-memory.

        :type parsed_statement: ParsedStatement
        :param parsed_statement: parsed statement containing sql query and query
         params
        """

        from google.cloud.spanner_dbapi import ProgrammingError

        if (
            parsed_statement.statement_type != StatementType.UPDATE
            and parsed_statement.statement_type != StatementType.INSERT
        ):
            raise ProgrammingError("Only DML statements are allowed in batch DML mode.")
        self._statements.append(parsed_statement.statement)

    def run_batch(self):
        """Executes all the buffered statements on the active dml batch by
        making a call to Spanner.
        """
        return run_batch_dml(self._cursor, self._statements)


def run_batch_dml(cursor: "Cursor", statements: List[Statement]):
    """Executes all the dml statements by making a batch call to Spanner.

    :type cursor: Cursor
    :param cursor: Database Cursor object

    :type statements: List[Statement]
    :param statements: list of statements to execute in batch
    """
    from google.cloud.spanner_dbapi import OperationalError

    many_result_set = StreamedManyResultSets()
    if not statements:
        return many_result_set
    connection = cursor.connection
    statements_tuple = []
    for statement in statements:
        statements_tuple.append(statement.get_tuple())
    if not connection._client_transaction_started:
        res = connection.database.run_in_transaction(_do_batch_update, statements_tuple)
        many_result_set.add_iter(res)
        cursor._row_count = sum([max(val, 0) for val in res])
    else:
        while True:
            try:
                transaction = connection.transaction_checkout()
                status, res = transaction.batch_update(statements_tuple)
                if status.code == ABORTED:
                    connection._transaction = None
                    raise Aborted(status.message)
                elif status.code != OK:
                    raise OperationalError(status.message)

                cursor._batch_dml_rows_count = res
                many_result_set.add_iter(res)
                cursor._row_count = sum([max(val, 0) for val in res])
                return many_result_set
            except Aborted:
                # We are raising it so it could be handled in transaction_helper.py and is retried
                if cursor._in_retry_mode:
                    raise
                else:
                    connection._transaction_helper.retry_transaction()


def _do_batch_update(transaction, statements):
    from google.cloud.spanner_dbapi import OperationalError

    status, res = transaction.batch_update(statements)
    if status.code == ABORTED:
        raise Aborted(status.message)
    elif status.code != OK:
        raise OperationalError(status.message)
    return res


class BatchMode(Enum):
    DML = 1
    DDL = 2
    NONE = 3
