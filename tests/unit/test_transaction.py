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
import datetime

from google.api_core.retry import Retry
from google.api_core import gapic_v1

from google.cloud.spanner_v1 import (
    CommitRequest,
    CommitResponse,
    RequestOptions,
    TransactionOptions,
    Type,
    TypeCode,
    ResultSetStats,
)

from google.cloud.spanner_v1.transaction import Transaction

from tests._builders import (
    build_precommit_token_pb,
    build_result_set_metadata_pb,
    build_result_set_pb,
    build_transaction,
    build_transaction_pb,
)
from tests._helpers import (
    HAS_OPENTELEMETRY_INSTALLED,
    LIB_VERSION,
    OpenTelemetryBase,
    StatusCode,
)

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
PARAM_TYPES = {"age": Type(code=TypeCode.INT64)}

TRANSACTION_ID = b"transaction-id"

PRECOMMIT_TOKEN_0 = build_precommit_token_pb(precommit_token=b"0", seq_num=0)
PRECOMMIT_TOKEN_1 = build_precommit_token_pb(precommit_token=b"1", seq_num=1)
PRECOMMIT_TOKEN_2 = build_precommit_token_pb(precommit_token=b"2", seq_num=2)

TRANSACTION_TAG = "transaction-tag"


class TestTransaction(OpenTelemetryBase):
    def test_ctor_session_w_existing_txn(self):
        transaction = build_transaction()
        with self.assertRaises(ValueError):
            Transaction(transaction._session)

    def test_ctor_defaults(self):
        from tests._builders import build_session

        session = build_session()
        transaction = Transaction(session=session)

        self.assertIs(transaction._session, session)
        self.assertIsNone(transaction._transaction_id)
        self.assertIsNone(transaction.committed)
        self.assertFalse(transaction.rolled_back)
        self.assertTrue(transaction._multi_use)
        self.assertEqual(transaction._execute_sql_request_count, 0)

    def test__check_state_already_committed(self):
        transaction = build_transaction()
        transaction.begin()
        transaction.commit()

        with self.assertRaises(ValueError):
            transaction._check_state()

    def test__check_state_already_rolled_back(self):
        transaction = build_transaction()
        transaction.begin()
        transaction.rollback()

        with self.assertRaises(ValueError):
            transaction._check_state()

    def test__check_state_ok(self):
        transaction = build_transaction()
        transaction._check_state()  # does not raise

        transaction.begin()
        transaction._check_state()  # does not raise

    def test__make_txn_selector(self):
        transaction = build_transaction()

        begin_transaction = transaction._session._database.spanner_api.begin_transaction
        begin_transaction.return_value = build_transaction_pb(id=TRANSACTION_ID)

        transaction.begin()

        selector = transaction._make_txn_selector()
        self.assertEqual(selector.id, TRANSACTION_ID)

    def test_begin_already_begun(self):
        transaction = build_transaction()
        transaction.begin()

        self.reset()
        with self.assertRaises(ValueError):
            transaction.begin()

        self.assertNoSpans()

    def test_begin_already_rolled_back(self):
        transaction = build_transaction()
        transaction.begin()
        transaction.rollback()

        self.reset()
        with self.assertRaises(ValueError):
            transaction.begin()

        self.assertNoSpans()

    def test_begin_already_committed(self):
        transaction = build_transaction()
        transaction.begin()
        transaction.commit()

        self.reset()
        with self.assertRaises(ValueError):
            transaction.begin()

        self.assertNoSpans()

    def test_begin_w_other_error(self):
        transaction = build_transaction()

        database = transaction._session._database
        database.spanner_api.begin_transaction.side_effect = RuntimeError()

        self.reset()
        with self.assertRaises(RuntimeError):
            transaction.begin()

        self.assertSpanAttributes(
            "CloudSpanner.Transaction.begin",
            status=StatusCode.ERROR,
            attributes=_build_base_attributes(database),
        )

    def test_begin_ok(self):
        transaction = build_transaction()
        session = transaction._session
        database = session._database

        api = database.spanner_api
        begin_transaction = api.begin_transaction
        begin_transaction.return_value = build_transaction_pb(id=TRANSACTION_ID)

        self.reset()
        transaction_id = transaction.begin()

        self.assertEqual(transaction_id, TRANSACTION_ID)
        self.assertEqual(transaction._transaction_id, TRANSACTION_ID)
        self.assertIsNone(transaction._precommit_token)

        begin_transaction.assert_called_once_with(
            session=session.name,
            options=TransactionOptions(read_write=TransactionOptions.ReadWrite()),
            metadata=[
                ("google-cloud-resource-prefix", database.name),
                ("x-goog-spanner-route-to-leader", "true"),
            ],
        )

        self.assertSpanAttributes(
            "CloudSpanner.Transaction.begin",
            attributes=_build_base_attributes(database),
        )

    def test_begin_w_retry(self):
        from google.api_core.exceptions import InternalServerError

        transaction = build_transaction()

        api = transaction._session._database.spanner_api
        begin_transaction = api.begin_transaction
        begin_transaction.side_effect = [
            InternalServerError("Received unexpected EOS on DATA frame from server"),
            build_transaction_pb(id=TRANSACTION_ID),
        ]

        transaction_id = transaction.begin()

        self.assertEqual(begin_transaction.call_count, 2)
        self.assertEqual(transaction_id, TRANSACTION_ID)
        self.assertEqual(transaction._transaction_id, TRANSACTION_ID)

    def test_begin_w_precommit_token(self):
        transaction = build_transaction()

        api = transaction._session._database.spanner_api
        api.begin_transaction.return_value = build_transaction_pb(
            id=TRANSACTION_ID, precommit_token=PRECOMMIT_TOKEN_0
        )

        transaction.begin()

        self.assertEqual(transaction._precommit_token, PRECOMMIT_TOKEN_0)

    def test_rollback_not_begun(self):
        transaction = build_transaction()

        self.reset()
        transaction.rollback()
        self.assertTrue(transaction.rolled_back)

        # Since there was no transaction to be rolled back, rollback rpc is not called.
        api = transaction._session._database.spanner_api
        api.rollback.assert_not_called()

        self.assertNoSpans()

    def test_rollback_already_committed(self):
        transaction = build_transaction()
        transaction.begin()
        transaction.commit()

        self.reset()
        with self.assertRaises(ValueError):
            transaction.rollback()

        self.assertNoSpans()

    def test_rollback_already_rolled_back(self):
        transaction = build_transaction()
        transaction.rollback()

        self.reset()
        with self.assertRaises(ValueError):
            transaction.rollback()

        self.assertNoSpans()

    def test_rollback_w_other_error(self):
        transaction = build_transaction()

        transaction.begin()
        transaction.insert(TABLE_NAME, COLUMNS, VALUES)

        database = transaction._session._database
        database.spanner_api.rollback.side_effect = RuntimeError()

        self.reset()
        with self.assertRaises(RuntimeError):
            transaction.rollback()

        self.assertFalse(transaction.rolled_back)

        self.assertSpanAttributes(
            "CloudSpanner.Transaction.rollback",
            status=StatusCode.ERROR,
            attributes=_build_base_attributes(database),
        )

    def test_rollback_ok(self):
        transaction = build_transaction()
        session = transaction._session
        database = session._database
        api = database.spanner_api

        transaction.begin()
        transaction.replace(TABLE_NAME, COLUMNS, VALUES)

        self.reset()
        transaction.rollback()

        self.assertTrue(transaction.rolled_back)
        self.assertIsNone(session._transaction)

        api.rollback.assert_called_once_with(
            session=session.name,
            transaction_id=transaction._transaction_id,
            metadata=[
                ("google-cloud-resource-prefix", database.name),
                ("x-goog-spanner-route-to-leader", "true"),
            ],
        )

        self.assertSpanAttributes(
            "CloudSpanner.Transaction.rollback",
            attributes=_build_base_attributes(database),
        )

    def test_commit_not_begun(self):
        transaction = build_transaction()

        self.reset()
        with self.assertRaises(ValueError):
            transaction.commit()

        if not HAS_OPENTELEMETRY_INSTALLED:
            return

        span_list = self.get_finished_spans()
        got_span_names = [span.name for span in span_list]
        want_span_names = ["CloudSpanner.Transaction.commit"]
        assert got_span_names == want_span_names

        got_span_events_statuses = self.finished_spans_events_statuses()
        want_span_events_statuses = [
            (
                "exception",
                {
                    "exception.type": "ValueError",
                    "exception.message": "Transaction is not begun",
                    "exception.stacktrace": "EPHEMERAL",
                    "exception.escaped": "False",
                },
            )
        ]
        assert got_span_events_statuses == want_span_events_statuses

    def test_commit_already_committed(self):
        transaction = build_transaction()
        transaction.begin()
        transaction.commit()

        self.reset()
        with self.assertRaises(ValueError):
            transaction.commit()

        if not HAS_OPENTELEMETRY_INSTALLED:
            return

        span_list = self.get_finished_spans()
        got_span_names = [span.name for span in span_list]
        want_span_names = ["CloudSpanner.Transaction.commit"]
        assert got_span_names == want_span_names

        got_span_events_statuses = self.finished_spans_events_statuses()
        want_span_events_statuses = [
            (
                "exception",
                {
                    "exception.type": "ValueError",
                    "exception.message": "Transaction is already committed",
                    "exception.stacktrace": "EPHEMERAL",
                    "exception.escaped": "False",
                },
            )
        ]
        assert got_span_events_statuses == want_span_events_statuses

    def test_commit_already_rolled_back(self):
        transaction = build_transaction()
        transaction.rollback()

        self.reset()
        with self.assertRaises(ValueError):
            transaction.commit()

        if not HAS_OPENTELEMETRY_INSTALLED:
            return

        span_list = self.get_finished_spans()
        got_span_names = [span.name for span in span_list]
        want_span_names = ["CloudSpanner.Transaction.commit"]
        assert got_span_names == want_span_names

        got_span_events_statuses = self.finished_spans_events_statuses()
        want_span_events_statuses = [
            (
                "exception",
                {
                    "exception.type": "ValueError",
                    "exception.message": "Transaction is already rolled back",
                    "exception.stacktrace": "EPHEMERAL",
                    "exception.escaped": "False",
                },
            )
        ]
        assert got_span_events_statuses == want_span_events_statuses

    def test_commit_w_other_error(self):
        transaction = build_transaction()

        transaction.begin()
        transaction.replace(TABLE_NAME, COLUMNS, VALUES)

        database = transaction._session._database
        database.spanner_api.commit.side_effect = RuntimeError()

        self.reset()
        with self.assertRaises(RuntimeError):
            transaction.commit()

        self.assertIsNone(transaction.committed)

        self.assertSpanAttributes(
            "CloudSpanner.Transaction.commit",
            status=StatusCode.ERROR,
            attributes=dict(_build_base_attributes(database), num_mutations=1),
        )

    def _commit_helper(
        self,
        mutate=True,
        return_commit_stats=False,
        request_options=None,
        max_commit_delay_in=None,
    ):
        import datetime

        from google.cloud.spanner_v1 import CommitRequest
        from google.cloud.spanner_v1 import CommitResponse
        from google.cloud.spanner_v1.keyset import KeySet
        from google.cloud._helpers import UTC

        # [A] Build transaction
        # ---------------------

        transaction = build_transaction()
        session = transaction._session
        database = session._database
        api = database.spanner_api

        transaction.transaction_tag = TRANSACTION_TAG

        # Build response
        # --------------

        now = datetime.datetime.now(tz=UTC)

        # TODO - test retry where precommit token is returned
        response = CommitResponse(commit_timestamp=now)
        if return_commit_stats:
            response.commit_stats.mutation_count = 4

        commit = api.commit
        commit.return_value = response

        # [C] Execute commit
        # ------------------

        transaction.begin()

        if mutate:
            keys = [[0], [1], [2]]
            keyset = KeySet(keys=keys)
            transaction.delete(TABLE_NAME, keyset)

        self.reset()
        transaction.commit(
            return_commit_stats=return_commit_stats,
            request_options=request_options,
            max_commit_delay=max_commit_delay_in,
        )

        # [D] Verify results
        # ------------------

        self.assertEqual(transaction.committed, now)
        self.assertIsNone(session._transaction)

        if request_options is None:
            expected_request_options = RequestOptions(transaction_tag=TRANSACTION_TAG)
        elif type(request_options) is dict:
            expected_request_options = RequestOptions(request_options)
            expected_request_options.transaction_tag = TRANSACTION_TAG
            expected_request_options.request_tag = None
        else:
            expected_request_options = request_options
            expected_request_options.transaction_tag = TRANSACTION_TAG
            expected_request_options.request_tag = None

        expected_request = CommitRequest(
            session=session.name,
            transaction_id=transaction._transaction_id,
            mutations=transaction._mutations,
            return_commit_stats=return_commit_stats,
            max_commit_delay=max_commit_delay_in,
            request_options=expected_request_options,
            precommit_token=transaction._precommit_token,
        )
        expected_metadata = [
            ("google-cloud-resource-prefix", database.name),
            ("x-goog-spanner-route-to-leader", "true"),
        ]

        commit.assert_called_once_with(
            request=expected_request,
            metadata=expected_metadata,
        )

        if return_commit_stats:
            self.assertEqual(transaction.commit_stats.mutation_count, 4)

        if not HAS_OPENTELEMETRY_INSTALLED:
            return

        span_list = self.get_finished_spans()
        got_span_names = [span.name for span in span_list]
        want_span_names = ["CloudSpanner.Transaction.commit"]
        assert got_span_names == want_span_names

        got_span_events_statuses = self.finished_spans_events_statuses()
        want_span_events_statuses = [("Starting Commit", {}), ("Commit Done", {})]
        assert got_span_events_statuses == want_span_events_statuses

    def test_commit_no_mutations(self):
        self._commit_helper(mutate=False)

    def test_commit_w_mutations(self):
        self._commit_helper(mutate=True)

    def test_commit_w_return_commit_stats(self):
        self._commit_helper(return_commit_stats=True)

    def test_commit_w_max_commit_delay(self):
        import datetime

        self._commit_helper(max_commit_delay_in=datetime.timedelta(milliseconds=100))

    def test_commit_w_request_tag_success(self):
        request_options = RequestOptions(
            request_tag="tag-1",
        )
        self._commit_helper(request_options=request_options)

    def test_commit_w_transaction_tag_ignored_success(self):
        request_options = RequestOptions(
            transaction_tag="tag-1-1",
        )
        self._commit_helper(request_options=request_options)

    def test_commit_w_request_and_transaction_tag_success(self):
        request_options = RequestOptions(
            request_tag="tag-1",
            transaction_tag="tag-1-1",
        )
        self._commit_helper(request_options=request_options)

    def test_commit_w_request_and_transaction_tag_dictionary_success(self):
        request_options = {"request_tag": "tag-1", "transaction_tag": "tag-1-1"}
        self._commit_helper(request_options=request_options)

    def test_commit_w_incorrect_tag_dictionary_error(self):
        request_options = {"incorrect_tag": "tag-1-1"}
        with self.assertRaises(ValueError):
            self._commit_helper(request_options=request_options)

    def test__make_params_pb_w_params_w_param_types(self):
        from google.protobuf.struct_pb2 import Struct
        from google.cloud.spanner_v1._helpers import _make_value_pb

        transaction = build_transaction()
        params_pb = transaction._make_params_pb(PARAMS, PARAM_TYPES)

        expected_params = Struct(
            fields={key: _make_value_pb(value) for (key, value) in PARAMS.items()}
        )
        self.assertEqual(params_pb, expected_params)

    def test_execute_update_other_error(self):
        transaction = build_transaction()
        transaction.begin()

        api = transaction._session._database.spanner_api
        api.execute_sql.side_effect = RuntimeError()

        self.reset()
        with self.assertRaises(RuntimeError):
            transaction.execute_update(DML_QUERY)

    def _execute_update_helper(
        self,
        count=0,
        query_options=None,
        request_options=None,
        retry=gapic_v1.method.DEFAULT,
        timeout=gapic_v1.method.DEFAULT,
        begin=True,
        use_multiplexed=False,
    ):
        from google.protobuf.struct_pb2 import Struct
        from google.cloud.spanner_v1 import (
            ResultSetStats,
        )
        from google.cloud.spanner_v1 import TransactionSelector
        from google.cloud.spanner_v1._helpers import (
            _make_value_pb,
            _merge_query_options,
        )
        from google.cloud.spanner_v1 import ExecuteSqlRequest

        # [A] Build transaction
        # ---------------------

        transaction = build_transaction()
        session = transaction._session
        database = session._database
        api = database.spanner_api

        transaction.transaction_tag = TRANSACTION_TAG
        transaction._execute_sql_request_count = count

        if begin:
            transaction.begin()

        # [B] Build results
        # -----------------

        # If the transaction had not already begun, the first result set will include
        # metadata with information about the transaction. Precommit tokens will be
        # included in the result sets if the transaction is on a multiplexed session.
        transaction_id = TRANSACTION_ID if not begin else None
        metadata_pb = build_result_set_metadata_pb(transaction={"id": transaction_id})
        precommit_token_pb = PRECOMMIT_TOKEN_0 if use_multiplexed else None

        api.execute_sql.return_value = build_result_set_pb(
            stats=ResultSetStats(row_count_exact=1),
            metadata=metadata_pb,
            precommit_token=precommit_token_pb,
        )

        # [C] Execute SQL
        # ---------------

        MODE = 2  # PROFILE

        if request_options is None:
            request_options = RequestOptions()
        elif type(request_options) is dict:
            request_options = RequestOptions(request_options)

        self.reset()
        row_count = transaction.execute_update(
            DML_QUERY_WITH_PARAM,
            PARAMS,
            PARAM_TYPES,
            query_mode=MODE,
            query_options=query_options,
            request_options=request_options,
            retry=retry,
            timeout=timeout,
        )

        # [D] Verify results
        # ------------------

        self.assertEqual(row_count, 1)

        expected_transaction_selector_pb = (
            TransactionSelector(id=transaction._transaction_id)
            if begin
            else TransactionSelector(
                begin=TransactionOptions(read_write=TransactionOptions.ReadWrite())
            )
        )

        expected_params = Struct(
            fields={key: _make_value_pb(value) for (key, value) in PARAMS.items()}
        )

        expected_query_options = database._instance._client._query_options
        if query_options:
            expected_query_options = _merge_query_options(
                expected_query_options, query_options
            )
        expected_request_options = request_options
        expected_request_options.transaction_tag = TRANSACTION_TAG

        expected_request = ExecuteSqlRequest(
            session=session.name,
            sql=DML_QUERY_WITH_PARAM,
            transaction=expected_transaction_selector_pb,
            params=expected_params,
            param_types=PARAM_TYPES,
            query_mode=MODE,
            query_options=expected_query_options,
            request_options=request_options,
            seqno=count,
        )
        api.execute_sql.assert_called_once_with(
            request=expected_request,
            retry=retry,
            timeout=timeout,
            metadata=[
                ("google-cloud-resource-prefix", database.name),
                ("x-goog-spanner-route-to-leader", "true"),
            ],
        )

        self.assertEqual(transaction._execute_sql_request_count, count + 1)
        want_span_attributes = dict(_build_base_attributes(database))
        want_span_attributes["db.statement"] = DML_QUERY_WITH_PARAM
        self.assertSpanAttributes(
            "CloudSpanner.Transaction.execute_update",
            status=StatusCode.OK,
            attributes=want_span_attributes,
        )

        if not begin:
            self.assertEqual(transaction._transaction_id, TRANSACTION_ID)

        if use_multiplexed:
            self.assertEqual(transaction._precommit_token, PRECOMMIT_TOKEN_0)

    def test_execute_update_wo_begin(self):
        self._execute_update_helper(begin=False)

    def test_execute_update_w_request_tag_success(self):
        request_options = RequestOptions(
            request_tag="tag-1",
        )
        self._execute_update_helper(request_options=request_options)

    def test_execute_update_w_transaction_tag_success(self):
        request_options = RequestOptions(
            transaction_tag="tag-1-1",
        )
        self._execute_update_helper(request_options=request_options)

    def test_execute_update_w_request_and_transaction_tag_success(self):
        request_options = RequestOptions(
            request_tag="tag-1",
            transaction_tag="tag-1-1",
        )
        self._execute_update_helper(request_options=request_options)

    def test_execute_update_w_request_and_transaction_tag_dictionary_success(self):
        request_options = {"request_tag": "tag-1", "transaction_tag": "tag-1-1"}
        self._execute_update_helper(request_options=request_options)

    def test_execute_update_w_incorrect_tag_dictionary_error(self):
        request_options = {"incorrect_tag": "tag-1-1"}
        with self.assertRaises(ValueError):
            self._execute_update_helper(request_options=request_options)

    def test_execute_update_w_count(self):
        self._execute_update_helper(count=1)

    def test_execute_update_w_timeout_param(self):
        self._execute_update_helper(timeout=2.0)

    def test_execute_update_w_retry_param(self):
        self._execute_update_helper(retry=Retry(deadline=60))

    def test_execute_update_w_timeout_and_retry_params(self):
        self._execute_update_helper(retry=Retry(deadline=60), timeout=2.0)

    def test_execute_update_error(self):
        transaction = build_transaction()

        api = transaction._session._database.spanner_api
        api.execute_sql.side_effect = RuntimeError()

        with self.assertRaises(RuntimeError):
            transaction.execute_update(DML_QUERY)

        self.assertEqual(transaction._execute_sql_request_count, 1)

    def test_execute_update_w_query_options(self):
        from google.cloud.spanner_v1 import ExecuteSqlRequest

        self._execute_update_helper(
            query_options=ExecuteSqlRequest.QueryOptions(optimizer_version="3")
        )

    def test_execute_update_w_request_options(self):
        self._execute_update_helper(
            request_options=RequestOptions(
                priority=RequestOptions.Priority.PRIORITY_MEDIUM
            )
        )

    def test_execute_update_w_precommit_token(self):
        self._execute_update_helper(use_multiplexed=True)

    def _batch_update_helper(
        self,
        error_after=None,
        count=0,
        request_options=None,
        retry=gapic_v1.method.DEFAULT,
        timeout=gapic_v1.method.DEFAULT,
        begin=True,
        use_multiplexed=False,
    ):
        from google.rpc.status_pb2 import Status
        from google.protobuf.struct_pb2 import Struct
        from google.cloud.spanner_v1 import param_types
        from google.cloud.spanner_v1 import ExecuteBatchDmlRequest
        from google.cloud.spanner_v1 import ExecuteBatchDmlResponse
        from google.cloud.spanner_v1 import TransactionSelector
        from google.cloud.spanner_v1._helpers import _make_value_pb

        # [A] Build transaction
        # ---------------------

        transaction = build_transaction()
        session = transaction._session
        database = session._database
        api = database.spanner_api

        transaction.transaction_tag = TRANSACTION_TAG
        transaction._execute_sql_request_count = count

        if begin:
            transaction.begin()

        # [B] Build results
        # -----------------

        insert_dml = "INSERT INTO table(pkey, desc) VALUES (%pkey, %desc)"
        insert_params = {"pkey": 12345, "desc": "DESCRIPTION"}
        insert_param_types = {"pkey": param_types.INT64, "desc": param_types.STRING}
        update_dml = 'UPDATE table SET desc = desc + "-amended"'
        delete_dml = "DELETE FROM table WHERE desc IS NULL"

        dml_statements = [
            (insert_dml, insert_params, insert_param_types),
            update_dml,
            delete_dml,
        ]

        # These precommit tokens are intentionally returned with sequence numbers out of order.
        precommit_tokens = [PRECOMMIT_TOKEN_2, PRECOMMIT_TOKEN_0, PRECOMMIT_TOKEN_1]

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

        result_sets = []
        for i in range(len(stats_pbs)):
            result_set_args = {"stats": stats_pbs[i]}

            # If the transaction had not already begun, the first result
            # set will include metadata with information about the transaction.
            if not begin and i == 0:
                result_set_args["metadata"] = build_result_set_metadata_pb(
                    transaction={"id": TRANSACTION_ID}
                )

            # Precommit tokens will be included in the result
            # sets if the transaction is on a multiplexed session.
            if use_multiplexed:
                result_set_args["precommit_token"] = precommit_tokens[i]

            result_sets.append(build_result_set_pb(**result_set_args))

        api.execute_batch_dml.return_value = ExecuteBatchDmlResponse(
            status=expected_status,
            result_sets=result_sets,
        )

        # [C] Execute batch DML
        # ---------------------

        if request_options is None:
            request_options = RequestOptions()
        elif type(request_options) is dict:
            request_options = RequestOptions(request_options)

        self.reset()
        status, row_counts = transaction.batch_update(
            dml_statements,
            request_options=request_options,
            retry=retry,
            timeout=timeout,
        )

        # [D] Verify results
        # ------------------

        self.assertEqual(status, expected_status)
        self.assertEqual(row_counts, expected_row_counts)

        expected_transaction_selector_pb = (
            TransactionSelector(id=transaction._transaction_id)
            if begin
            else TransactionSelector(
                begin=TransactionOptions(read_write=TransactionOptions.ReadWrite())
            )
        )

        expected_insert_params = Struct(
            fields={
                key: _make_value_pb(value) for (key, value) in insert_params.items()
            }
        )
        expected_statements = [
            ExecuteBatchDmlRequest.Statement(
                sql=insert_dml,
                params=expected_insert_params,
                param_types=insert_param_types,
            ),
            ExecuteBatchDmlRequest.Statement(sql=update_dml),
            ExecuteBatchDmlRequest.Statement(sql=delete_dml),
        ]
        expected_request_options = request_options
        expected_request_options.transaction_tag = TRANSACTION_TAG

        expected_request = ExecuteBatchDmlRequest(
            session=session.name,
            transaction=expected_transaction_selector_pb,
            statements=expected_statements,
            seqno=count,
            request_options=expected_request_options,
        )
        api.execute_batch_dml.assert_called_once_with(
            request=expected_request,
            metadata=[
                ("google-cloud-resource-prefix", database.name),
                ("x-goog-spanner-route-to-leader", "true"),
            ],
            retry=retry,
            timeout=timeout,
        )

        self.assertEqual(transaction._execute_sql_request_count, count + 1)

        if not begin:
            self.assertEqual(transaction._transaction_id, TRANSACTION_ID)

        if use_multiplexed:
            self.assertEqual(transaction._precommit_token, PRECOMMIT_TOKEN_2)

    def test_batch_update_wo_begin(self):
        self._batch_update_helper(begin=False)

    def test_batch_update_wo_errors(self):
        self._batch_update_helper(
            request_options=RequestOptions(
                priority=RequestOptions.Priority.PRIORITY_MEDIUM
            ),
        )

    def test_batch_update_w_request_tag_success(self):
        request_options = RequestOptions(
            request_tag="tag-1",
        )
        self._batch_update_helper(request_options=request_options)

    def test_batch_update_w_transaction_tag_success(self):
        request_options = RequestOptions(
            transaction_tag="tag-1-1",
        )
        self._batch_update_helper(request_options=request_options)

    def test_batch_update_w_request_and_transaction_tag_success(self):
        request_options = RequestOptions(
            request_tag="tag-1",
            transaction_tag="tag-1-1",
        )
        self._batch_update_helper(request_options=request_options)

    def test_batch_update_w_request_and_transaction_tag_dictionary_success(self):
        request_options = {"request_tag": "tag-1", "transaction_tag": "tag-1-1"}
        self._batch_update_helper(request_options=request_options)

    def test_batch_update_w_incorrect_tag_dictionary_error(self):
        request_options = {"incorrect_tag": "tag-1-1"}
        with self.assertRaises(ValueError):
            self._batch_update_helper(request_options=request_options)

    def test_batch_update_w_errors(self):
        self._batch_update_helper(error_after=2, count=1)

    def test_batch_update_error(self):
        from google.cloud.spanner_v1 import Type
        from google.cloud.spanner_v1 import TypeCode

        transaction = build_transaction()
        transaction.begin()

        api = transaction._session._database.spanner_api
        api.execute_batch_dml.side_effect = RuntimeError()

        insert_dml = "INSERT INTO table(pkey, desc) VALUES (%pkey, %desc)"
        insert_params = {"pkey": 12345, "desc": "DESCRIPTION"}
        insert_param_types = {
            "pkey": Type(code=TypeCode.INT64),
            "desc": Type(code=TypeCode.STRING),
        }
        update_dml = 'UPDATE table SET desc = desc + "-amended"'
        delete_dml = "DELETE FROM table WHERE desc IS NULL"

        dml_statements = [
            (insert_dml, insert_params, insert_param_types),
            update_dml,
            delete_dml,
        ]

        with self.assertRaises(RuntimeError):
            transaction.batch_update(dml_statements)

        self.assertEqual(transaction._execute_sql_request_count, 1)

    def test_batch_update_w_timeout_param(self):
        self._batch_update_helper(timeout=2.0)

    def test_batch_update_w_retry_param(self):
        self._batch_update_helper(retry=gapic_v1.method.DEFAULT)

    def test_batch_update_w_timeout_and_retry_params(self):
        self._batch_update_helper(retry=gapic_v1.method.DEFAULT, timeout=2.0)

    def test_batch_update_w_precommit_token(self):
        self._batch_update_helper(use_multiplexed=True)

    def test_context_mgr_success(self):
        transaction = build_transaction()
        session = transaction._session
        database = session._database
        api = database.spanner_api

        begin = api.begin_transaction
        transaction_id = TRANSACTION_ID
        begin.return_value = build_transaction_pb(id=transaction_id)

        commit = api.commit
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        commit.return_value = CommitResponse(commit_timestamp=now)

        with transaction:
            transaction.insert(TABLE_NAME, COLUMNS, VALUES)

        self.assertEqual(transaction.committed, now)

        expected_request = CommitRequest(
            session=session.name,
            transaction_id=transaction_id,
            mutations=transaction._mutations,
            request_options=RequestOptions(),
        )
        expected_metadata = [
            ("google-cloud-resource-prefix", database.name),
            ("x-goog-spanner-route-to-leader", "true"),
        ]

        commit.assert_called_once_with(
            request=expected_request,
            metadata=expected_metadata,
        )

    def test_context_mgr_failure(self):
        transaction = build_transaction()

        with self.assertRaises(Exception):
            with transaction:
                transaction.insert(TABLE_NAME, COLUMNS, VALUES)
                raise Exception("bail out")

        self.assertEqual(transaction.committed, None)
        # Rollback rpc will not be called as there is no transaction id to be rolled back, rolled_back flag will be marked as true.
        self.assertTrue(transaction.rolled_back)
        self.assertEqual(len(transaction._mutations), 1)

        api = transaction._session._database.spanner_api
        api.commit.assert_not_called()


def _build_base_attributes(database) -> dict:
    """Builds and returns the base attributes for the given database."""

    base_attributes = {
        "db.type": "spanner",
        "db.url": "spanner.googleapis.com",
        "db.instance": database.name,
        "net.host.name": "spanner.googleapis.com",
        "gcp.client.service": "spanner",
        "gcp.client.version": LIB_VERSION,
        "gcp.client.repo": "googleapis/python-spanner",
    }

    from tests._helpers import enrich_with_otel_scope

    enrich_with_otel_scope(base_attributes)

    return base_attributes
