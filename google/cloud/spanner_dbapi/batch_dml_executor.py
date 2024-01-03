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
from google.cloud.spanner_dbapi.checksum import ResultsChecksum
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

    def run_batch_dml(self):
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

    connection = cursor.connection
    many_result_set = StreamedManyResultSets()
    statements_tuple = []
    for statement in statements:
        statements_tuple.append(statement.get_tuple())
    if not connection._client_transaction_started:
        res = connection.database.run_in_transaction(_do_batch_update, statements_tuple)
        many_result_set.add_iter(res)
        cursor._row_count = sum([max(val, 0) for val in res])
    else:
        retried = False
        while True:
            try:
                transaction = connection.transaction_checkout()
                status, res = transaction.batch_update(statements_tuple)
                many_result_set.add_iter(res)
                res_checksum = ResultsChecksum()
                res_checksum.consume_result(res)
                res_checksum.consume_result(status.code)
                if not retried:
                    connection._statements.append((statements, res_checksum))
                cursor._row_count = sum([max(val, 0) for val in res])

                if status.code == ABORTED:
                    connection._transaction = None
                    raise Aborted(status.message)
                elif status.code != OK:
                    raise OperationalError(status.message)
                return many_result_set
            except Aborted:
                connection.retry_transaction()
                retried = True


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
