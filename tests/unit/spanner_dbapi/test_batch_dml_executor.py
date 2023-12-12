import unittest
from unittest import mock

from google.cloud.spanner_dbapi import ProgrammingError
from google.cloud.spanner_dbapi.batch_dml_executor import BatchDmlExecutor
from google.cloud.spanner_dbapi.parsed_statement import (
    ParsedStatement,
    Statement,
    StatementType,
)


class TestBatchDmlExecutor(unittest.TestCase):
    @mock.patch("google.cloud.spanner_dbapi.cursor.Cursor")
    def setUp(self, mock_cursor):
        self._under_test = BatchDmlExecutor(mock_cursor)

    def test_execute_statement_non_dml_statement_type(self):
        parsed_statement = ParsedStatement(StatementType.QUERY, Statement("sql"))

        with self.assertRaises(ProgrammingError):
            self._under_test.execute_statement(parsed_statement)

    def test_execute_statement_insert_statement_type(self):
        statement = Statement("sql")

        self._under_test.execute_statement(
            ParsedStatement(StatementType.INSERT, statement)
        )

        self.assertEqual(self._under_test._statements, [statement])

    def test_execute_statement_update_statement_type(self):
        statement = Statement("sql")

        self._under_test.execute_statement(
            ParsedStatement(StatementType.UPDATE, statement)
        )

        self.assertEqual(self._under_test._statements, [statement])
