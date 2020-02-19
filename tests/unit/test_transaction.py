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


import unittest

import mock


TABLE_NAME = "citizens"
COLUMNS = ["email", "first_name", "last_name", "age"]
VALUES = [
    ["phred@exammple.com", "Phred", "Phlyntstone", 32],
    ["bharney@example.com", "Bharney", "Rhubble", 31],
]
DML_QUERY = """\
INSERT INTO citizens(first_name, last_name, age)
VALUES ("Phred", "Phlyntstone", 32)
"""
DML_QUERY_WITH_PARAM = """
INSERT INTO citizens(first_name, last_name, age)
VALUES ("Phred", "Phlyntstone", @age)
"""
PARAMS = {"age": 30}
PARAM_TYPES = {"age": "INT64"}


class TestTransaction(unittest.TestCase):

    PROJECT_ID = "project-id"
    INSTANCE_ID = "instance-id"
    INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID
    DATABASE_ID = "database-id"
    DATABASE_NAME = INSTANCE_NAME + "/databases/" + DATABASE_ID
    SESSION_ID = "session-id"
    SESSION_NAME = DATABASE_NAME + "/sessions/" + SESSION_ID
    TRANSACTION_ID = b"DEADBEEF"

    def _getTargetClass(self):
        from google.cloud.spanner_v1.transaction import Transaction

        return Transaction

    def _make_one(self, session, *args, **kwargs):
        transaction = self._getTargetClass()(session, *args, **kwargs)
        session._transaction = transaction
        return transaction

    def _make_spanner_api(self):
        import google.cloud.spanner_v1.gapic.spanner_client

        return mock.create_autospec(
            google.cloud.spanner_v1.gapic.spanner_client.SpannerClient, instance=True
        )

    def test_ctor_session_w_existing_txn(self):
        session = _Session()
        session._transaction = object()
        with self.assertRaises(ValueError):
            self._make_one(session)

    def test_ctor_defaults(self):
        session = _Session()
        transaction = self._make_one(session)
        self.assertIs(transaction._session, session)
        self.assertIsNone(transaction._transaction_id)
        self.assertIsNone(transaction.committed)
        self.assertFalse(transaction.rolled_back)
        self.assertTrue(transaction._multi_use)
        self.assertEqual(transaction._execute_sql_count, 0)

    def test__check_state_not_begun(self):
        session = _Session()
        transaction = self._make_one(session)
        with self.assertRaises(ValueError):
            transaction._check_state()

    def test__check_state_already_committed(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.committed = object()
        with self.assertRaises(ValueError):
            transaction._check_state()

    def test__check_state_already_rolled_back(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.rolled_back = True
        with self.assertRaises(ValueError):
            transaction._check_state()

    def test__check_state_ok(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction._check_state()  # does not raise

    def test__make_txn_selector(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        selector = transaction._make_txn_selector()
        self.assertEqual(selector.id, self.TRANSACTION_ID)

    def test_begin_already_begun(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        with self.assertRaises(ValueError):
            transaction.begin()

    def test_begin_already_rolled_back(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction.rolled_back = True
        with self.assertRaises(ValueError):
            transaction.begin()

    def test_begin_already_committed(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction.committed = object()
        with self.assertRaises(ValueError):
            transaction.begin()

    def test_begin_w_other_error(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.begin_transaction.side_effect = RuntimeError()
        session = _Session(database)
        transaction = self._make_one(session)

        with self.assertRaises(RuntimeError):
            transaction.begin()

    def test_begin_ok(self):
        from google.cloud.spanner_v1.proto.transaction_pb2 import (
            Transaction as TransactionPB,
        )

        transaction_pb = TransactionPB(id=self.TRANSACTION_ID)
        database = _Database()
        api = database.spanner_api = _FauxSpannerAPI(
            _begin_transaction_response=transaction_pb
        )
        session = _Session(database)
        transaction = self._make_one(session)

        txn_id = transaction.begin()

        self.assertEqual(txn_id, self.TRANSACTION_ID)
        self.assertEqual(transaction._transaction_id, self.TRANSACTION_ID)

        session_id, txn_options, metadata = api._begun
        self.assertEqual(session_id, session.name)
        self.assertTrue(txn_options.HasField("read_write"))
        self.assertEqual(metadata, [("google-cloud-resource-prefix", database.name)])

    def test_rollback_not_begun(self):
        session = _Session()
        transaction = self._make_one(session)
        with self.assertRaises(ValueError):
            transaction.rollback()

    def test_rollback_already_committed(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.committed = object()
        with self.assertRaises(ValueError):
            transaction.rollback()

    def test_rollback_already_rolled_back(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.rolled_back = True
        with self.assertRaises(ValueError):
            transaction.rollback()

    def test_rollback_w_other_error(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.rollback.side_effect = RuntimeError("other error")
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.insert(TABLE_NAME, COLUMNS, VALUES)

        with self.assertRaises(RuntimeError):
            transaction.rollback()

        self.assertFalse(transaction.rolled_back)

    def test_rollback_ok(self):
        from google.protobuf.empty_pb2 import Empty

        empty_pb = Empty()
        database = _Database()
        api = database.spanner_api = _FauxSpannerAPI(_rollback_response=empty_pb)
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.replace(TABLE_NAME, COLUMNS, VALUES)

        transaction.rollback()

        self.assertTrue(transaction.rolled_back)
        self.assertIsNone(session._transaction)

        session_id, txn_id, metadata = api._rolled_back
        self.assertEqual(session_id, session.name)
        self.assertEqual(txn_id, self.TRANSACTION_ID)
        self.assertEqual(metadata, [("google-cloud-resource-prefix", database.name)])

    def test_commit_not_begun(self):
        session = _Session()
        transaction = self._make_one(session)
        with self.assertRaises(ValueError):
            transaction.commit()

    def test_commit_already_committed(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.committed = object()
        with self.assertRaises(ValueError):
            transaction.commit()

    def test_commit_already_rolled_back(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.rolled_back = True
        with self.assertRaises(ValueError):
            transaction.commit()

    def test_commit_w_other_error(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.commit.side_effect = RuntimeError()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction.replace(TABLE_NAME, COLUMNS, VALUES)

        with self.assertRaises(RuntimeError):
            transaction.commit()

        self.assertIsNone(transaction.committed)

    def _commit_helper(self, mutate=True):
        import datetime
        from google.cloud.spanner_v1.proto.spanner_pb2 import CommitResponse
        from google.cloud.spanner_v1.keyset import KeySet
        from google.cloud._helpers import UTC
        from google.cloud._helpers import _datetime_to_pb_timestamp

        now = datetime.datetime.utcnow().replace(tzinfo=UTC)
        now_pb = _datetime_to_pb_timestamp(now)
        keys = [[0], [1], [2]]
        keyset = KeySet(keys=keys)
        response = CommitResponse(commit_timestamp=now_pb)
        database = _Database()
        api = database.spanner_api = _FauxSpannerAPI(_commit_response=response)
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID

        if mutate:
            transaction.delete(TABLE_NAME, keyset)

        transaction.commit()

        self.assertEqual(transaction.committed, now)
        self.assertIsNone(session._transaction)

        session_id, mutations, txn_id, metadata = api._committed
        self.assertEqual(session_id, session.name)
        self.assertEqual(txn_id, self.TRANSACTION_ID)
        self.assertEqual(mutations, transaction._mutations)
        self.assertEqual(metadata, [("google-cloud-resource-prefix", database.name)])

    def test_commit_no_mutations(self):
        self._commit_helper(mutate=False)

    def test_commit_w_mutations(self):
        self._commit_helper(mutate=True)

    def test__make_params_pb_w_params_wo_param_types(self):
        session = _Session()
        transaction = self._make_one(session)

        with self.assertRaises(ValueError):
            transaction._make_params_pb(PARAMS, None)

    def test__make_params_pb_wo_params_w_param_types(self):
        session = _Session()
        transaction = self._make_one(session)

        with self.assertRaises(ValueError):
            transaction._make_params_pb(None, PARAM_TYPES)

    def test__make_params_pb_w_params_w_param_types(self):
        from google.protobuf.struct_pb2 import Struct
        from google.cloud.spanner_v1._helpers import _make_value_pb

        session = _Session()
        transaction = self._make_one(session)

        params_pb = transaction._make_params_pb(PARAMS, PARAM_TYPES)

        expected_params = Struct(
            fields={key: _make_value_pb(value) for (key, value) in PARAMS.items()}
        )
        self.assertEqual(params_pb, expected_params)

    def test_execute_update_other_error(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.execute_sql.side_effect = RuntimeError()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID

        with self.assertRaises(RuntimeError):
            transaction.execute_update(DML_QUERY)

    def test_execute_update_w_params_wo_param_types(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        session = _Session(database)
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID

        with self.assertRaises(ValueError):
            transaction.execute_update(DML_QUERY_WITH_PARAM, PARAMS)

    def _execute_update_helper(self, count=0):
        from google.protobuf.struct_pb2 import Struct
        from google.cloud.spanner_v1.proto.result_set_pb2 import (
            ResultSet,
            ResultSetStats,
        )
        from google.cloud.spanner_v1.proto.transaction_pb2 import TransactionSelector
        from google.cloud.spanner_v1._helpers import _make_value_pb

        MODE = 2  # PROFILE
        stats_pb = ResultSetStats(row_count_exact=1)
        database = _Database()
        api = database.spanner_api = self._make_spanner_api()
        api.execute_sql.return_value = ResultSet(stats=stats_pb)
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction._execute_sql_count = count

        row_count = transaction.execute_update(
            DML_QUERY_WITH_PARAM, PARAMS, PARAM_TYPES, query_mode=MODE
        )

        self.assertEqual(row_count, 1)

        expected_transaction = TransactionSelector(id=self.TRANSACTION_ID)
        expected_params = Struct(
            fields={key: _make_value_pb(value) for (key, value) in PARAMS.items()}
        )

        api.execute_sql.assert_called_once_with(
            self.SESSION_NAME,
            DML_QUERY_WITH_PARAM,
            transaction=expected_transaction,
            params=expected_params,
            param_types=PARAM_TYPES,
            query_mode=MODE,
            seqno=count,
            metadata=[("google-cloud-resource-prefix", database.name)],
        )

        self.assertEqual(transaction._execute_sql_count, count + 1)

    def test_execute_update_new_transaction(self):
        self._execute_update_helper()

    def test_execute_update_w_count(self):
        self._execute_update_helper(count=1)

    def test_batch_update_other_error(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.execute_batch_dml.side_effect = RuntimeError()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID

        with self.assertRaises(RuntimeError):
            transaction.batch_update(statements=[DML_QUERY])

    def _batch_update_helper(self, error_after=None, count=0):
        from google.rpc.status_pb2 import Status
        from google.protobuf.struct_pb2 import Struct
        from google.cloud.spanner_v1.proto.result_set_pb2 import ResultSet
        from google.cloud.spanner_v1.proto.result_set_pb2 import ResultSetStats
        from google.cloud.spanner_v1.proto.spanner_pb2 import ExecuteBatchDmlResponse
        from google.cloud.spanner_v1.proto.transaction_pb2 import TransactionSelector
        from google.cloud.spanner_v1._helpers import _make_value_pb

        insert_dml = "INSERT INTO table(pkey, desc) VALUES (%pkey, %desc)"
        insert_params = {"pkey": 12345, "desc": "DESCRIPTION"}
        insert_param_types = {"pkey": "INT64", "desc": "STRING"}
        update_dml = 'UPDATE table SET desc = desc + "-amended"'
        delete_dml = "DELETE FROM table WHERE desc IS NULL"

        dml_statements = [
            (insert_dml, insert_params, insert_param_types),
            update_dml,
            delete_dml,
        ]

        stats_pbs = [
            ResultSetStats(row_count_exact=1),
            ResultSetStats(row_count_exact=2),
            ResultSetStats(row_count_exact=3),
        ]
        if error_after is not None:
            stats_pbs = stats_pbs[:error_after]
            expected_status = Status(code=400)
        else:
            expected_status = Status(code=200)
        expected_row_counts = [stats.row_count_exact for stats in stats_pbs]

        response = ExecuteBatchDmlResponse(
            status=expected_status,
            result_sets=[ResultSet(stats=stats_pb) for stats_pb in stats_pbs],
        )
        database = _Database()
        api = database.spanner_api = self._make_spanner_api()
        api.execute_batch_dml.return_value = response
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = self.TRANSACTION_ID
        transaction._execute_sql_count = count

        status, row_counts = transaction.batch_update(dml_statements)

        self.assertEqual(status, expected_status)
        self.assertEqual(row_counts, expected_row_counts)

        expected_transaction = TransactionSelector(id=self.TRANSACTION_ID)
        expected_insert_params = Struct(
            fields={
                key: _make_value_pb(value) for (key, value) in insert_params.items()
            }
        )
        expected_statements = [
            {
                "sql": insert_dml,
                "params": expected_insert_params,
                "param_types": insert_param_types,
            },
            {"sql": update_dml},
            {"sql": delete_dml},
        ]

        api.execute_batch_dml.assert_called_once_with(
            session=self.SESSION_NAME,
            transaction=expected_transaction,
            statements=expected_statements,
            seqno=count,
            metadata=[("google-cloud-resource-prefix", database.name)],
        )

        self.assertEqual(transaction._execute_sql_count, count + 1)

    def test_batch_update_wo_errors(self):
        self._batch_update_helper()

    def test_batch_update_w_errors(self):
        self._batch_update_helper(error_after=2, count=1)

    def test_context_mgr_success(self):
        import datetime
        from google.cloud.spanner_v1.proto.spanner_pb2 import CommitResponse
        from google.cloud.spanner_v1.proto.transaction_pb2 import (
            Transaction as TransactionPB,
        )
        from google.cloud._helpers import UTC
        from google.cloud._helpers import _datetime_to_pb_timestamp

        transaction_pb = TransactionPB(id=self.TRANSACTION_ID)
        database = _Database()
        now = datetime.datetime.utcnow().replace(tzinfo=UTC)
        now_pb = _datetime_to_pb_timestamp(now)
        response = CommitResponse(commit_timestamp=now_pb)
        database = _Database()
        api = database.spanner_api = _FauxSpannerAPI(
            _begin_transaction_response=transaction_pb, _commit_response=response
        )
        session = _Session(database)
        transaction = self._make_one(session)

        with transaction:
            transaction.insert(TABLE_NAME, COLUMNS, VALUES)

        self.assertEqual(transaction.committed, now)

        session_id, mutations, txn_id, metadata = api._committed
        self.assertEqual(session_id, self.SESSION_NAME)
        self.assertEqual(txn_id, self.TRANSACTION_ID)
        self.assertEqual(mutations, transaction._mutations)
        self.assertEqual(metadata, [("google-cloud-resource-prefix", database.name)])

    def test_context_mgr_failure(self):
        from google.protobuf.empty_pb2 import Empty

        empty_pb = Empty()
        from google.cloud.spanner_v1.proto.transaction_pb2 import (
            Transaction as TransactionPB,
        )

        transaction_pb = TransactionPB(id=self.TRANSACTION_ID)
        database = _Database()
        api = database.spanner_api = _FauxSpannerAPI(
            _begin_transaction_response=transaction_pb, _rollback_response=empty_pb
        )
        session = _Session(database)
        transaction = self._make_one(session)

        with self.assertRaises(Exception):
            with transaction:
                transaction.insert(TABLE_NAME, COLUMNS, VALUES)
                raise Exception("bail out")

        self.assertEqual(transaction.committed, None)
        self.assertTrue(transaction.rolled_back)
        self.assertEqual(len(transaction._mutations), 1)

        self.assertEqual(api._committed, None)

        session_id, txn_id, metadata = api._rolled_back
        self.assertEqual(session_id, session.name)
        self.assertEqual(txn_id, self.TRANSACTION_ID)
        self.assertEqual(metadata, [("google-cloud-resource-prefix", database.name)])


class _Database(object):
    name = "testing"


class _Session(object):

    _transaction = None

    def __init__(self, database=None, name=TestTransaction.SESSION_NAME):
        self._database = database
        self.name = name


class _FauxSpannerAPI(object):

    _committed = None

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

    def begin_transaction(self, session, options_, metadata=None):
        self._begun = (session, options_, metadata)
        return self._begin_transaction_response

    def rollback(self, session, transaction_id, metadata=None):
        self._rolled_back = (session, transaction_id, metadata)
        return self._rollback_response

    def commit(
        self,
        session,
        mutations,
        transaction_id="",
        single_use_transaction=None,
        metadata=None,
    ):
        assert single_use_transaction is None
        self._committed = (session, mutations, transaction_id, metadata)
        return self._commit_response
