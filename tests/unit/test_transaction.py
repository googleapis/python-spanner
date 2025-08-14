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
from threading import Lock
from typing import Mapping
from datetime import timedelta

import mock

from google.cloud.spanner_v1 import (
    RequestOptions,
    CommitRequest,
    Mutation,
    KeySet,
    BeginTransactionRequest,
    TransactionOptions,
    ResultSetMetadata,
)
from google.cloud.spanner_v1 import DefaultTransactionOptions
from google.cloud.spanner_v1 import Type
from google.cloud.spanner_v1 import TypeCode
from google.api_core.retry import Retry
from google.api_core import gapic_v1
from google.cloud.spanner_v1._helpers import (
    AtomicCounter,
    _metadata_with_request_id,
)
from google.cloud.spanner_v1.batch import _make_write_pb
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.transaction import Transaction
from google.cloud.spanner_v1.request_id_header import (
    REQ_RAND_PROCESS_ID,
    build_request_id,
)
from tests._builders import (
    build_transaction,
    build_precommit_token_pb,
    build_session,
    build_commit_response_pb,
    build_transaction_pb,
)

from tests._helpers import (
    HAS_OPENTELEMETRY_INSTALLED,
    LIB_VERSION,
    OpenTelemetryBase,
    StatusCode,
    enrich_with_otel_scope,
)

KEYS = [[0], [1], [2]]
KEYSET = KeySet(keys=KEYS)
KEYSET_PB = KEYSET._to_pb()

TABLE_NAME = "citizens"
COLUMNS = ["email", "first_name", "last_name", "age"]
VALUE_1 = ["phred@exammple.com", "Phred", "Phlyntstone", 32]
VALUE_2 = ["bharney@example.com", "Bharney", "Rhubble", 31]
VALUES = [VALUE_1, VALUE_2]

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
TRANSACTION_TAG = "transaction-tag"

PRECOMMIT_TOKEN_PB_0 = build_precommit_token_pb(precommit_token=b"0", seq_num=0)
PRECOMMIT_TOKEN_PB_1 = build_precommit_token_pb(precommit_token=b"1", seq_num=1)
PRECOMMIT_TOKEN_PB_2 = build_precommit_token_pb(precommit_token=b"2", seq_num=2)

DELETE_MUTATION = Mutation(delete=Mutation.Delete(table=TABLE_NAME, key_set=KEYSET_PB))
INSERT_MUTATION = Mutation(insert=_make_write_pb(TABLE_NAME, COLUMNS, VALUES))
UPDATE_MUTATION = Mutation(update=_make_write_pb(TABLE_NAME, COLUMNS, VALUES))


class TestTransaction(OpenTelemetryBase):
    PROJECT_ID = "project-id"
    INSTANCE_ID = "instance-id"
    INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID
    DATABASE_ID = "database-id"
    DATABASE_NAME = INSTANCE_NAME + "/databases/" + DATABASE_ID
    SESSION_ID = "session-id"
    SESSION_NAME = DATABASE_NAME + "/sessions/" + SESSION_ID

    def _getTargetClass(self):
        from google.cloud.spanner_v1.transaction import Transaction

        return Transaction

    def _make_one(self, session, *args, **kwargs):
        transaction = self._getTargetClass()(session, *args, **kwargs)
        session._transaction = transaction
        return transaction

    def _make_spanner_api(self):
        from google.cloud.spanner_v1 import SpannerClient

        return mock.create_autospec(SpannerClient, instance=True)

    def test_ctor_defaults(self):
        session = build_session()
        transaction = Transaction(session=session)

        # Attributes from _SessionWrapper
        self.assertEqual(transaction._session, session)

        # Attributes from _SnapshotBase
        self.assertFalse(transaction._read_only)
        self.assertTrue(transaction._multi_use)
        self.assertEqual(transaction._execute_sql_request_count, 0)
        self.assertEqual(transaction._read_request_count, 0)
        self.assertIsNone(transaction._transaction_id)
        self.assertIsNone(transaction._precommit_token)
        self.assertIsInstance(transaction._lock, type(Lock()))

        # Attributes from _BatchBase
        self.assertEqual(transaction._mutations, [])
        self.assertIsNone(transaction._precommit_token)
        self.assertIsNone(transaction.committed)
        self.assertIsNone(transaction.commit_stats)

        self.assertFalse(transaction.rolled_back)

    def test_begin_already_rolled_back(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction.rolled_back = True
        with self.assertRaises(ValueError):
            transaction.begin()

        self.assertNoSpans()

    def test_begin_already_committed(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction.committed = object()
        with self.assertRaises(ValueError):
            transaction.begin()

        self.assertNoSpans()

    def test_rollback_not_begun(self):
        database = _Database()
        api = database.spanner_api = self._make_spanner_api()
        session = _Session(database)
        transaction = self._make_one(session)

        transaction.rollback()
        self.assertTrue(transaction.rolled_back)

        # Since there was no transaction to be rolled back, rollback rpc is not called.
        api.rollback.assert_not_called()

        self.assertNoSpans()

    def test_rollback_already_committed(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID
        transaction.committed = object()
        with self.assertRaises(ValueError):
            transaction.rollback()

        self.assertNoSpans()

    def test_rollback_already_rolled_back(self):
        session = _Session()
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID
        transaction.rolled_back = True
        with self.assertRaises(ValueError):
            transaction.rollback()

        self.assertNoSpans()

    def test_rollback_w_other_error(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.rollback.side_effect = RuntimeError("other error")
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID
        transaction.insert(TABLE_NAME, COLUMNS, VALUES)

        with self.assertRaises(RuntimeError):
            transaction.rollback()

        self.assertFalse(transaction.rolled_back)

        req_id = f"1.{REQ_RAND_PROCESS_ID}.{database._nth_client_id}.{database._channel_id}.1.1"
        self.assertSpanAttributes(
            "CloudSpanner.Transaction.rollback",
            status=StatusCode.ERROR,
            attributes=self._build_span_attributes(
                database, x_goog_spanner_request_id=req_id
            ),
        )

    def test_rollback_ok(self):
        from google.protobuf.empty_pb2 import Empty

        empty_pb = Empty()
        database = _Database()
        api = database.spanner_api = _FauxSpannerAPI(_rollback_response=empty_pb)
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID
        transaction.replace(TABLE_NAME, COLUMNS, VALUES)

        transaction.rollback()

        self.assertTrue(transaction.rolled_back)

        session_id, txn_id, metadata = api._rolled_back
        self.assertEqual(session_id, session.name)
        self.assertEqual(txn_id, TRANSACTION_ID)
        req_id = f"1.{REQ_RAND_PROCESS_ID}.{database._nth_client_id}.{database._channel_id}.1.1"
        self.assertEqual(
            metadata,
            [
                ("google-cloud-resource-prefix", database.name),
                ("x-goog-spanner-route-to-leader", "true"),
                (
                    "x-goog-spanner-request-id",
                    req_id,
                ),
            ],
        )

        self.assertSpanAttributes(
            "CloudSpanner.Transaction.rollback",
            attributes=self._build_span_attributes(
                database, x_goog_spanner_request_id=req_id
            ),
        )

    def test_commit_not_begun(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        session = _Session(database)
        transaction = self._make_one(session)
        with self.assertRaises(ValueError):
            transaction.commit()

        if not HAS_OPENTELEMETRY_INSTALLED:
            return

        span_list = self.get_finished_spans()
        got_span_names = [span.name for span in span_list]
        want_span_names = ["CloudSpanner.Transaction.commit"]
        self.assertEqual(got_span_names, want_span_names)

        got_span_events_statuses = self.finished_spans_events_statuses()
        want_span_events_statuses = [
            (
                "exception",
                {
                    "exception.type": "ValueError",
                    "exception.message": "Transaction has not begun.",
                    "exception.stacktrace": "EPHEMERAL",
                    "exception.escaped": "False",
                },
            )
        ]
        self.assertEqual(got_span_events_statuses, want_span_events_statuses)

    def test_commit_already_committed(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID
        transaction.committed = object()
        with self.assertRaises(ValueError):
            transaction.commit()

        if not HAS_OPENTELEMETRY_INSTALLED:
            return

        span_list = self.get_finished_spans()
        got_span_names = [span.name for span in span_list]
        want_span_names = ["CloudSpanner.Transaction.commit"]
        self.assertEqual(got_span_names, want_span_names)

        got_span_events_statuses = self.finished_spans_events_statuses()
        want_span_events_statuses = [
            (
                "exception",
                {
                    "exception.type": "ValueError",
                    "exception.message": "Transaction already committed.",
                    "exception.stacktrace": "EPHEMERAL",
                    "exception.escaped": "False",
                },
            )
        ]
        self.assertEqual(got_span_events_statuses, want_span_events_statuses)

    def test_commit_already_rolled_back(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID
        transaction.rolled_back = True
        with self.assertRaises(ValueError):
            transaction.commit()

        if not HAS_OPENTELEMETRY_INSTALLED:
            return

        span_list = self.get_finished_spans()
        got_span_names = [span.name for span in span_list]
        want_span_names = ["CloudSpanner.Transaction.commit"]
        self.assertEqual(got_span_names, want_span_names)

        got_span_events_statuses = self.finished_spans_events_statuses()
        want_span_events_statuses = [
            (
                "exception",
                {
                    "exception.type": "ValueError",
                    "exception.message": "Transaction already rolled back.",
                    "exception.stacktrace": "EPHEMERAL",
                    "exception.escaped": "False",
                },
            )
        ]
        self.assertEqual(got_span_events_statuses, want_span_events_statuses)

    def test_commit_w_other_error(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.commit.side_effect = RuntimeError()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID
        transaction.replace(TABLE_NAME, COLUMNS, VALUES)

        with self.assertRaises(RuntimeError):
            transaction.commit()

        self.assertIsNone(transaction.committed)

        req_id = f"1.{REQ_RAND_PROCESS_ID}.{_Client.NTH_CLIENT.value}.1.1.1"
        self.assertSpanAttributes(
            "CloudSpanner.Transaction.commit",
            status=StatusCode.ERROR,
            attributes=self._build_span_attributes(
                database,
                x_goog_spanner_request_id=req_id,
                num_mutations=1,
            ),
        )

    def _commit_helper(
        self,
        mutations=None,
        return_commit_stats=False,
        request_options=None,
        max_commit_delay_in=None,
        retry_for_precommit_token=None,
        is_multiplexed=False,
        expected_begin_mutation=None,
    ):
        from google.cloud.spanner_v1 import CommitRequest

        # [A] Build transaction
        # ---------------------

        session = build_session(is_multiplexed=is_multiplexed)
        transaction = build_transaction(session=session)

        database = session._database
        api = database.spanner_api

        transaction.transaction_tag = TRANSACTION_TAG

        if mutations is not None:
            transaction._mutations = mutations

        # [B] Build responses
        # -------------------

        # Mock begin API call.
        begin_precommit_token_pb = PRECOMMIT_TOKEN_PB_0
        begin_transaction = api.begin_transaction
        begin_transaction.return_value = build_transaction_pb(
            id=TRANSACTION_ID, precommit_token=begin_precommit_token_pb
        )

        # Mock commit API call.
        retry_precommit_token = PRECOMMIT_TOKEN_PB_1
        commit_response_pb = build_commit_response_pb(
            precommit_token=retry_precommit_token if retry_for_precommit_token else None
        )
        if return_commit_stats:
            commit_response_pb.commit_stats.mutation_count = 4

        commit = api.commit
        commit.return_value = commit_response_pb

        # [C] Begin transaction, add mutations, and execute commit
        # --------------------------------------------------------

        # Transaction must be begun unless it is mutations-only.
        if mutations is None:
            transaction._transaction_id = TRANSACTION_ID

        commit_timestamp = transaction.commit(
            return_commit_stats=return_commit_stats,
            request_options=request_options,
            max_commit_delay=max_commit_delay_in,
        )

        # [D] Verify results
        # ------------------

        # Verify transaction state.
        self.assertEqual(transaction.committed, commit_timestamp)

        if return_commit_stats:
            self.assertEqual(transaction.commit_stats.mutation_count, 4)

        nth_request_counter = AtomicCounter()
        base_metadata = [
            ("google-cloud-resource-prefix", database.name),
            ("x-goog-spanner-route-to-leader", "true"),
        ]

        # Verify begin API call.
        if mutations is not None:
            self.assertEqual(transaction._transaction_id, TRANSACTION_ID)

            expected_begin_transaction_request = BeginTransactionRequest(
                session=session.name,
                options=TransactionOptions(read_write=TransactionOptions.ReadWrite()),
                mutation_key=expected_begin_mutation,
            )

            expected_begin_metadata = base_metadata.copy()
            expected_begin_metadata.append(
                (
                    "x-goog-spanner-request-id",
                    self._build_request_id(
                        database, nth_request=nth_request_counter.increment()
                    ),
                )
            )

            begin_transaction.assert_called_once_with(
                request=expected_begin_transaction_request,
                metadata=expected_begin_metadata,
            )

        # Verify commit API call(s).
        self.assertEqual(commit.call_count, 1 if not retry_for_precommit_token else 2)

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

        common_expected_commit_response_args = {
            "session": session.name,
            "transaction_id": TRANSACTION_ID,
            "return_commit_stats": return_commit_stats,
            "max_commit_delay": max_commit_delay_in,
            "request_options": expected_request_options,
        }

        # Only include precommit_token if the session is multiplexed and token exists
        commit_request_args = {
            "mutations": transaction._mutations,
            **common_expected_commit_response_args,
        }
        if session.is_multiplexed and transaction._precommit_token is not None:
            commit_request_args["precommit_token"] = transaction._precommit_token

        expected_commit_request = CommitRequest(**commit_request_args)

        expected_commit_metadata = base_metadata.copy()
        expected_commit_metadata.append(
            (
                "x-goog-spanner-request-id",
                self._build_request_id(
                    database, nth_request=nth_request_counter.increment()
                ),
            )
        )
        commit.assert_any_call(
            request=expected_commit_request,
            metadata=expected_commit_metadata,
        )

        if retry_for_precommit_token:
            expected_retry_request = CommitRequest(
                precommit_token=retry_precommit_token,
                **common_expected_commit_response_args,
            )
            expected_retry_metadata = base_metadata.copy()
            expected_retry_metadata.append(
                (
                    "x-goog-spanner-request-id",
                    self._build_request_id(
                        database, nth_request=nth_request_counter.increment()
                    ),
                )
            )
            commit.assert_any_call(
                request=expected_retry_request,
                metadata=expected_retry_metadata,
            )

        if not HAS_OPENTELEMETRY_INSTALLED:
            return

        # Verify span names.
        expected_names = ["CloudSpanner.Transaction.commit"]
        if mutations is not None:
            expected_names.append("CloudSpanner.Transaction.begin")

        actual_names = [span.name for span in self.get_finished_spans()]
        self.assertEqual(actual_names, expected_names)

        # Verify span events statuses.
        expected_statuses = [("Starting Commit", {})]
        if retry_for_precommit_token:
            expected_statuses.append(
                ("Transaction Commit Attempt Failed. Retrying", {})
            )
        expected_statuses.append(("Commit Done", {}))

        actual_statuses = self.finished_spans_events_statuses()
        self.assertEqual(actual_statuses, expected_statuses)

    def test_commit_mutations_only_not_multiplexed(self):
        self._commit_helper(mutations=[DELETE_MUTATION], is_multiplexed=False)

    def test_commit_mutations_only_multiplexed_w_non_insert_mutation(self):
        self._commit_helper(
            mutations=[DELETE_MUTATION],
            is_multiplexed=True,
            expected_begin_mutation=DELETE_MUTATION,
        )

    def test_commit_mutations_only_multiplexed_w_insert_mutation(self):
        self._commit_helper(
            mutations=[INSERT_MUTATION],
            is_multiplexed=True,
            expected_begin_mutation=INSERT_MUTATION,
        )

    def test_commit_mutations_only_multiplexed_w_non_insert_and_insert_mutations(self):
        self._commit_helper(
            mutations=[INSERT_MUTATION, DELETE_MUTATION],
            is_multiplexed=True,
            expected_begin_mutation=DELETE_MUTATION,
        )

    def test_commit_mutations_only_multiplexed_w_multiple_insert_mutations(self):
        insert_1 = Mutation(insert=_make_write_pb(TABLE_NAME, COLUMNS, [VALUE_1]))
        insert_2 = Mutation(
            insert=_make_write_pb(TABLE_NAME, COLUMNS, [VALUE_1, VALUE_2])
        )

        self._commit_helper(
            mutations=[insert_1, insert_2],
            is_multiplexed=True,
            expected_begin_mutation=insert_2,
        )

    def test_commit_mutations_only_multiplexed_w_multiple_non_insert_mutations(self):
        mutations = [UPDATE_MUTATION, DELETE_MUTATION]
        self._commit_helper(
            mutations=mutations,
            is_multiplexed=True,
            expected_begin_mutation=mutations[0],
        )

    def test_commit_w_return_commit_stats(self):
        self._commit_helper(return_commit_stats=True)

    def test_commit_w_max_commit_delay(self):
        self._commit_helper(max_commit_delay_in=timedelta(milliseconds=100))

    def test_commit_w_request_tag_success(self):
        request_options = RequestOptions(request_tag="tag-1")
        self._commit_helper(request_options=request_options)

    def test_commit_w_transaction_tag_ignored_success(self):
        request_options = RequestOptions(transaction_tag="tag-1-1")
        self._commit_helper(request_options=request_options)

    def test_commit_w_request_and_transaction_tag_success(self):
        request_options = RequestOptions(request_tag="tag-1", transaction_tag="tag-1-1")
        self._commit_helper(request_options=request_options)

    def test_commit_w_request_and_transaction_tag_dictionary_success(self):
        request_options = {"request_tag": "tag-1", "transaction_tag": "tag-1-1"}
        self._commit_helper(request_options=request_options)

    def test_commit_w_incorrect_tag_dictionary_error(self):
        request_options = {"incorrect_tag": "tag-1-1"}
        with self.assertRaises(ValueError):
            self._commit_helper(request_options=request_options)

    def test_commit_w_retry_for_precommit_token(self):
        self._commit_helper(retry_for_precommit_token=True)

    def test_commit_w_retry_for_precommit_token_then_error(self):
        transaction = build_transaction()

        commit = transaction._session._database.spanner_api.commit
        commit.side_effect = [
            build_commit_response_pb(precommit_token=PRECOMMIT_TOKEN_PB_0),
            RuntimeError(),
        ]

        transaction.begin()
        with self.assertRaises(RuntimeError):
            transaction.commit()

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
        transaction._transaction_id = TRANSACTION_ID

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
            ResultSet,
            ResultSetStats,
        )
        from google.cloud.spanner_v1 import TransactionSelector
        from google.cloud.spanner_v1._helpers import (
            _make_value_pb,
            _merge_query_options,
        )
        from google.cloud.spanner_v1 import ExecuteSqlRequest

        MODE = 2  # PROFILE
        database = _Database()
        api = database.spanner_api = self._make_spanner_api()

        # If the transaction had not already begun, the first result set will include
        # metadata with information about the transaction. Precommit tokens will be
        # included in the result sets if the transaction is on a multiplexed session.
        transaction_pb = None if begin else build_transaction_pb(id=TRANSACTION_ID)
        metadata_pb = ResultSetMetadata(transaction=transaction_pb)
        precommit_token_pb = PRECOMMIT_TOKEN_PB_0 if use_multiplexed else None

        api.execute_sql.return_value = ResultSet(
            stats=ResultSetStats(row_count_exact=1),
            metadata=metadata_pb,
            precommit_token=precommit_token_pb,
        )

        session = _Session(database)
        transaction = self._make_one(session)
        transaction.transaction_tag = TRANSACTION_TAG
        transaction._execute_sql_request_count = count

        if begin:
            transaction._transaction_id = TRANSACTION_ID

        if request_options is None:
            request_options = RequestOptions()
        elif type(request_options) is dict:
            request_options = RequestOptions(request_options)

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

        self.assertEqual(row_count, 1)

        expected_transaction = (
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
            session=self.SESSION_NAME,
            sql=DML_QUERY_WITH_PARAM,
            transaction=expected_transaction,
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
                (
                    "x-goog-spanner-request-id",
                    f"1.{REQ_RAND_PROCESS_ID}.{_Client.NTH_CLIENT.value}.1.1.1",
                ),
            ],
        )

        self.assertSpanAttributes(
            "CloudSpanner.Transaction.execute_update",
            attributes=self._build_span_attributes(
                database, **{"db.statement": DML_QUERY_WITH_PARAM}
            ),
        )

        self.assertEqual(transaction._transaction_id, TRANSACTION_ID)
        self.assertEqual(transaction._execute_sql_request_count, count + 1)

        if use_multiplexed:
            self.assertEqual(transaction._precommit_token, PRECOMMIT_TOKEN_PB_0)

    def test_execute_update_new_transaction(self):
        self._execute_update_helper()

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
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.execute_sql.side_effect = RuntimeError()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID

        with self.assertRaises(RuntimeError):
            transaction.execute_update(DML_QUERY)

        self.assertEqual(transaction._execute_sql_request_count, 1)

    def test_execute_update_w_query_options(self):
        from google.cloud.spanner_v1 import ExecuteSqlRequest

        self._execute_update_helper(
            query_options=ExecuteSqlRequest.QueryOptions(optimizer_version="3")
        )

    def test_execute_update_wo_begin(self):
        self._execute_update_helper(begin=False)

    def test_execute_update_w_precommit_token(self):
        self._execute_update_helper(use_multiplexed=True)

    def test_execute_update_w_request_options(self):
        self._execute_update_helper(
            request_options=RequestOptions(
                priority=RequestOptions.Priority.PRIORITY_MEDIUM
            )
        )

    def test_batch_update_other_error(self):
        database = _Database()
        database.spanner_api = self._make_spanner_api()
        database.spanner_api.execute_batch_dml.side_effect = RuntimeError()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID

        with self.assertRaises(RuntimeError):
            transaction.batch_update(statements=[DML_QUERY])

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
        from google.cloud.spanner_v1 import ResultSet
        from google.cloud.spanner_v1 import ExecuteBatchDmlRequest
        from google.cloud.spanner_v1 import ExecuteBatchDmlResponse
        from google.cloud.spanner_v1 import TransactionSelector
        from google.cloud.spanner_v1._helpers import _make_value_pb

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

        # These precommit tokens are intentionally returned with sequence numbers out
        # of order to test that the transaction saves the precommit token with the
        # highest sequence number.
        precommit_tokens = [
            PRECOMMIT_TOKEN_PB_2,
            PRECOMMIT_TOKEN_PB_0,
            PRECOMMIT_TOKEN_PB_1,
        ]

        expected_status = Status(code=200) if error_after is None else Status(code=400)

        result_sets = []
        for i in range(len(precommit_tokens)):
            if error_after is not None and i == error_after:
                break

            result_set_args = {"stats": {"row_count_exact": i}}

            # If the transaction had not already begun, the first result
            # set will include metadata with information about the transaction.
            if not begin and i == 0:
                result_set_args["metadata"] = {"transaction": {"id": TRANSACTION_ID}}

            # Precommit tokens will be included in the result
            # sets if the transaction is on a multiplexed session.
            if use_multiplexed:
                result_set_args["precommit_token"] = precommit_tokens[i]

            result_sets.append(ResultSet(**result_set_args))

        database = _Database()
        api = database.spanner_api = self._make_spanner_api()
        api.execute_batch_dml.return_value = ExecuteBatchDmlResponse(
            status=expected_status,
            result_sets=result_sets,
        )

        session = _Session(database)
        transaction = self._make_one(session)
        transaction.transaction_tag = TRANSACTION_TAG
        transaction._execute_sql_request_count = count

        if begin:
            transaction._transaction_id = TRANSACTION_ID

        if request_options is None:
            request_options = RequestOptions()
        elif type(request_options) is dict:
            request_options = RequestOptions(request_options)

        status, row_counts = transaction.batch_update(
            dml_statements,
            request_options=request_options,
            retry=retry,
            timeout=timeout,
        )

        self.assertEqual(status, expected_status)
        self.assertEqual(
            row_counts, [result_set.stats.row_count_exact for result_set in result_sets]
        )

        expected_transaction = (
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
            session=self.SESSION_NAME,
            transaction=expected_transaction,
            statements=expected_statements,
            seqno=count,
            request_options=expected_request_options,
        )
        api.execute_batch_dml.assert_called_once_with(
            request=expected_request,
            metadata=[
                ("google-cloud-resource-prefix", database.name),
                ("x-goog-spanner-route-to-leader", "true"),
                (
                    "x-goog-spanner-request-id",
                    f"1.{REQ_RAND_PROCESS_ID}.{_Client.NTH_CLIENT.value}.1.1.1",
                ),
            ],
            retry=retry,
            timeout=timeout,
        )

        self.assertEqual(transaction._execute_sql_request_count, count + 1)
        self.assertEqual(transaction._transaction_id, TRANSACTION_ID)

        if use_multiplexed:
            self.assertEqual(transaction._precommit_token, PRECOMMIT_TOKEN_PB_2)

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

        database = _Database()
        api = database.spanner_api = self._make_spanner_api()
        api.execute_batch_dml.side_effect = RuntimeError()
        session = _Session(database)
        transaction = self._make_one(session)
        transaction._transaction_id = TRANSACTION_ID

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
        commit = database.spanner_api.commit

        with transaction:
            transaction.insert(TABLE_NAME, COLUMNS, VALUES)

        self.assertEqual(transaction.committed, commit.return_value.commit_timestamp)

        commit.assert_called_once_with(
            request=CommitRequest(
                session=session.name,
                transaction_id=transaction._transaction_id,
                request_options=RequestOptions(),
                mutations=transaction._mutations,
            ),
            metadata=[
                ("google-cloud-resource-prefix", database.name),
                ("x-goog-spanner-route-to-leader", "true"),
                ("x-goog-spanner-request-id", self._build_request_id(database)),
            ],
        )

    def test_context_mgr_failure(self):
        from google.protobuf.empty_pb2 import Empty

        empty_pb = Empty()
        from google.cloud.spanner_v1 import Transaction as TransactionPB

        transaction_pb = TransactionPB(id=TRANSACTION_ID)
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
        # Rollback rpc will not be called as there is no transaction id to be rolled back, rolled_back flag will be marked as true.
        self.assertTrue(transaction.rolled_back)
        self.assertEqual(len(transaction._mutations), 1)
        self.assertEqual(api._committed, None)

    @staticmethod
    def _build_span_attributes(
        database: Database, **extra_attributes
    ) -> Mapping[str, str]:
        """Builds the attributes for spans using the given database and extra attributes."""

        attributes = enrich_with_otel_scope(
            {
                "db.type": "spanner",
                "db.url": "spanner.googleapis.com",
                "db.instance": database.name,
                "net.host.name": "spanner.googleapis.com",
                "gcp.client.service": "spanner",
                "gcp.client.version": LIB_VERSION,
                "gcp.client.repo": "googleapis/python-spanner",
            }
        )

        if extra_attributes:
            attributes.update(extra_attributes)

        return attributes

    @staticmethod
    def _build_request_id(
        database: Database, nth_request: int = None, attempt: int = 1
    ) -> str:
        """Builds a request ID for an Spanner Client API request with the given database and attempt number."""

        client = database._instance._client
        nth_request = nth_request or client._nth_request.value

        return build_request_id(
            client_id=client._nth_client_id,
            channel_id=database._channel_id,
            nth_request=nth_request,
            attempt=attempt,
        )


class _Client(object):
    NTH_CLIENT = AtomicCounter()

    def __init__(self):
        from google.cloud.spanner_v1 import ExecuteSqlRequest

        self._query_options = ExecuteSqlRequest.QueryOptions(optimizer_version="1")
        self.directed_read_options = None
        self._nth_client_id = _Client.NTH_CLIENT.increment()
        self._nth_request = AtomicCounter()

    @property
    def _next_nth_request(self):
        return self._nth_request.increment()


class _Instance(object):
    def __init__(self):
        self._client = _Client()


class _Database(object):
    def __init__(self):
        self.name = "testing"
        self._instance = _Instance()
        self._route_to_leader_enabled = True
        self._directed_read_options = None
        self.default_transaction_options = DefaultTransactionOptions()

    @property
    def _next_nth_request(self):
        return self._instance._client._next_nth_request

    @property
    def _nth_client_id(self):
        return self._instance._client._nth_client_id

    def metadata_with_request_id(
        self, nth_request, nth_attempt, prior_metadata=[], span=None
    ):
        return _metadata_with_request_id(
            self._nth_client_id,
            self._channel_id,
            nth_request,
            nth_attempt,
            prior_metadata,
            span,
        )

    @property
    def _channel_id(self):
        return 1


class _Session(object):
    _transaction = None

    def __init__(self, database=None, name=TestTransaction.SESSION_NAME):
        self._database = database
        self.name = name

    @property
    def session_id(self):
        return self.name


class _FauxSpannerAPI(object):
    _committed = None

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

    def begin_transaction(self, session=None, options=None, metadata=None):
        self._begun = (session, options, metadata)
        return self._begin_transaction_response

    def rollback(self, session=None, transaction_id=None, metadata=None):
        self._rolled_back = (session, transaction_id, metadata)
        return self._rollback_response

    def commit(
        self,
        request=None,
        metadata=None,
    ):
        assert not request.single_use_transaction

        max_commit_delay = None
        if type(request).pb(request).HasField("max_commit_delay"):
            max_commit_delay = request.max_commit_delay

        self._committed = (
            request.session,
            request.mutations,
            request.transaction_id,
            request.request_options,
            max_commit_delay,
            metadata,
        )
        return self._commit_response
