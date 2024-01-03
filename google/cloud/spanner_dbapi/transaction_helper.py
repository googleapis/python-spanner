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
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, List, Union, Any, Dict
from google.api_core.exceptions import Aborted

import time

from google.cloud.spanner_dbapi.batch_dml_executor import BatchMode
from google.cloud.spanner_dbapi.exceptions import RetryAborted
from google.cloud.spanner_dbapi.parsed_statement import (
    StatementType,
    ClientSideStatementType,
)
from google.cloud.spanner_v1.session import _get_retry_delay

if TYPE_CHECKING:
    from google.cloud.spanner_dbapi import Connection, Cursor
from google.cloud.spanner_dbapi.checksum import ResultsChecksum, _compare_checksums

MAX_INTERNAL_RETRIES = 50
RETRY_ABORTED_ERROR = "The transaction was aborted and could not be retried due to a concurrent modification."


class TransactionRetryHelper:
    def __init__(self, connection: "Connection"):
        """Helper class used in retrying the transaction when aborted This will
        maintain all the statements executed on original transaction and replay
        them again in the retried transaction.

        :type connection: :class:`~google.cloud.spanner_dbapi.connection.Connection`
        :param connection: A DB-API connection to Google Cloud Spanner.
        """

        self._connection = connection
        # list of all single and batch statements in the same order as executed
        # in original transaction along with their checksum value
        self._statement_result_details_list: List[StatementDetails] = []
        # last StatementDetails that was added in the _statement_result_details_list
        self._last_statement_result_details: StatementDetails = None
        # 1-1 map from original cursor object on which transaction ran to the
        # new cursor object used in the retry
        self._cursor_map: Dict[Cursor, Cursor] = {}

    def _set_connection_for_retry(self):
        self._connection._spanner_transaction_started = False
        self._connection._transaction_begin_marked = False
        self._connection._batch_mode = BatchMode.NONE

    def reset(self):
        """
        Resets the state of the class when the ongoing transaction is committed
        or aborted
        """
        self._statement_result_details_list = []
        self._last_statement_result_details = None
        self._cursor_map = {}

    def add_fetch_statement_for_retry(
        self, cursor, result_rows, exception, is_fetch_all
    ):
        """
        StatementDetails to be added to _statement_result_details_list whenever fetchone, fetchmany or
        fetchall method is called on the cursor.
        If fetchone is consecutively called n times then it is stored as fetchmany with size as n.
        Same for fetchmany, so consecutive fetchone and fetchmany statements are stored as one
        fetchmany statement in _statement_result_details_list with size param appropriately set

        :param cursor: original Cursor object on which statement executed in the transaction
        :param result_rows: All the rows from the resultSet from fetch statement execution
        :param exception: Not none in case non-aborted exception is thrown on the original
        statement execution
        :param is_fetch_all: True in case of fetchall statement execution
        """
        if not self._connection._client_transaction_started:
            return
        if (
            self._last_statement_result_details is not None
            and self._last_statement_result_details.statement_type
            == CursorStatementType.FETCH_MANY
            and self._last_statement_result_details.cursor == cursor
        ):
            if exception is not None:
                self._last_statement_result_details.result_details = exception
            else:
                for row in result_rows:
                    self._last_statement_result_details.result_details.consume_result(
                        row
                    )
                self._last_statement_result_details.size += len(result_rows)
        else:
            result_details = _get_statement_result_checksum(result_rows)
            if is_fetch_all:
                self._last_statement_result_details = FetchStatement(
                    cursor=cursor,
                    statement_type=CursorStatementType.FETCH_ALL,
                    result_details=result_details,
                )
            else:
                self._last_statement_result_details = FetchStatement(
                    cursor=cursor,
                    statement_type=CursorStatementType.FETCH_MANY,
                    result_details=result_details,
                    size=len(result_rows),
                )
            self._statement_result_details_list.append(
                self._last_statement_result_details
            )

    def add_execute_statement_for_retry(
        self, cursor, sql, args, exception, is_execute_many
    ):
        """
        StatementDetails to be added to _statement_result_details_list whenever execute or
        executemany method is called on the cursor.

        :param cursor: original Cursor object on which statement executed in the transaction
        :param sql: Input param of the execute/executemany method
        :param args: Input param of the execute/executemany method
        :param exception: Not none in case non-aborted exception is thrown on the original
        statement execution
        :param is_execute_many: True in case of executemany statement execution
        """
        if not self._connection._client_transaction_started:
            return
        statement_type = CursorStatementType.EXECUTE
        if is_execute_many:
            statement_type = CursorStatementType.EXECUTE_MANY
        result_details = None
        parsed_statement = cursor._parsed_statement
        if exception is not None:
            result_details = exception
        elif (
            parsed_statement.statement_type == StatementType.INSERT
            or parsed_statement.statement_type == StatementType.UPDATE
            or parsed_statement.client_side_statement_type
            == ClientSideStatementType.RUN_BATCH
        ):
            result_details = cursor.rowcount

        self._last_statement_result_details = ExecuteStatement(
            cursor=cursor,
            statement_type=statement_type,
            sql=sql,
            args=args,
            result_details=result_details,
        )
        self._statement_result_details_list.append(self._last_statement_result_details)

    def retry_transaction(self):
        """Retry the aborted transaction.

        All the statements executed in the original transaction
        will be re-executed in new one. Results checksums of the
        original statements and the retried ones will be compared.

        :raises: :class:`google.cloud.spanner_dbapi.exceptions.RetryAborted`
            If results checksum of the retried statement is
            not equal to the checksum of the original one.
        """
        attempt = 0
        while True:
            attempt += 1
            if attempt > MAX_INTERNAL_RETRIES:
                raise

            self._set_connection_for_retry()

            try:
                for statement_result_details in self._statement_result_details_list:
                    if statement_result_details.cursor in self._cursor_map:
                        cursor = self._cursor_map.get(statement_result_details.cursor)
                    else:
                        cursor: Cursor = self._connection.cursor()
                        cursor._in_retry_mode = True
                        self._cursor_map[statement_result_details.cursor] = cursor
                    try:
                        self._handle_statement(statement_result_details, cursor)
                    except Aborted:
                        raise
                    except RetryAborted:
                        raise
                    except Exception as ex:
                        if (
                            type(statement_result_details.result_details)
                            is not type(ex)
                            or ex.args != statement_result_details.result_details.args
                        ):
                            raise RetryAborted(RETRY_ABORTED_ERROR, ex)
                return
            except Aborted as ex:
                delay = _get_retry_delay(ex.errors[0], attempt)
                if delay:
                    time.sleep(delay)

    def _handle_statement(self, statement_result_details, cursor):
        statement_type = statement_result_details.statement_type
        if statement_type == CursorStatementType.EXECUTE:
            cursor.execute(statement_result_details.sql, statement_result_details.args)
            if (
                type(statement_result_details.result_details) is int
                and statement_result_details.result_details != cursor.rowcount
            ):
                raise RetryAborted(RETRY_ABORTED_ERROR)
        elif statement_type == CursorStatementType.EXECUTE_MANY:
            cursor.executemany(
                statement_result_details.sql,
                statement_result_details.args,
            )
            if (
                type(statement_result_details.result_details) is int
                and statement_result_details.result_details != cursor.rowcount
            ):
                raise RetryAborted(RETRY_ABORTED_ERROR)
        elif statement_type == CursorStatementType.FETCH_ALL:
            res = cursor.fetchall()
            checksum = _get_statement_result_checksum(res)
            _compare_checksums(checksum, statement_result_details.result_details)
        elif statement_type == CursorStatementType.FETCH_MANY:
            res = cursor.fetchmany(statement_result_details.size)
            checksum = _get_statement_result_checksum(res)
            _compare_checksums(checksum, statement_result_details.result_details)


def _get_statement_result_checksum(res_iter):
    retried_checksum = ResultsChecksum()
    for res in res_iter:
        retried_checksum.consume_result(res)
    return retried_checksum


class CursorStatementType(Enum):
    EXECUTE = 1
    EXECUTE_MANY = 2
    FETCH_ONE = 3
    FETCH_ALL = 4
    FETCH_MANY = 5


@dataclass
class StatementDetails:
    statement_type: CursorStatementType
    # The cursor object on which this statement was executed
    cursor: "Cursor"
    # This would be one of
    # 1. checksum of ResultSet in case of fetch call on query statement
    # 2. Total rows updated in case of DML
    # 3. Exception details in case of statement execution throws exception
    # 4. None in case of execute calls
    result_details: Union[ResultsChecksum, int, Exception, None]


@dataclass
class ExecuteStatement(StatementDetails):
    sql: str
    args: Any = None


@dataclass
class FetchStatement(StatementDetails):
    size: int = None
