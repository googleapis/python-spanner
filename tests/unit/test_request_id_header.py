# Copyright 2024 Google LLC All rights reserved.
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

import random
import threading

from google.api_core.exceptions import Aborted
from google.rpc import code_pb2

from google.cloud.spanner_v1 import (
    BatchCreateSessionsRequest,
    BeginTransactionRequest,
    ExecuteSqlRequest,
)
from google.cloud.spanner_v1.request_id_header import REQ_RAND_PROCESS_ID
from tests.mockserver_tests.mock_server_test_base import (
    MockServerTestBase,
    add_select1_result,
)


class TestRequestIDHeader(MockServerTestBase):
    def tearDown(self):
        self.database._x_goog_request_id_interceptor.reset()

    def test_snapshot_execute_sql(self):
        add_select1_result()
        if not getattr(self.database, "_interceptors", None):
            self.database._interceptors = MockServerTestBase._interceptors
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql("select 1")
            result_list = []
            for row in results:
                result_list.append(row)
                self.assertEqual(1, row[0])
            self.assertEqual(1, len(result_list))

        requests = self.spanner_service.requests
        self.assertEqual(2, len(requests), msg=requests)
        self.assertTrue(isinstance(requests[0], BatchCreateSessionsRequest))
        self.assertTrue(isinstance(requests[1], ExecuteSqlRequest))

        NTH_CLIENT = self.database._nth_client_id
        CHANNEL_ID = self.database._channel_id
        # Now ensure monotonicity of the received request-id segments.
        got_stream_segments, got_unary_segments = self.canonicalize_request_id_headers()
        want_unary_segments = [
            (
                "/google.spanner.v1.Spanner/BatchCreateSessions",
                (1, REQ_RAND_PROCESS_ID, NTH_CLIENT, CHANNEL_ID, 1, 1),
            )
        ]
        want_stream_segments = [
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, NTH_CLIENT, CHANNEL_ID, 2, 1),
            )
        ]

        assert got_unary_segments == want_unary_segments
        assert got_stream_segments == want_stream_segments

    def test_snapshot_read_concurrent(self):
        def select1():
            with self.database.snapshot() as snapshot:
                rows = snapshot.execute_sql("select 1")
                res_list = []
                for row in rows:
                    self.assertEqual(1, row[0])
                    res_list.append(row)
                self.assertEqual(1, len(res_list))

        n = 10
        threads = []
        for i in range(n):
            th = threading.Thread(target=select1, name=f"snapshot-select1-{i}")
            th.run()
            threads.append(th)

        random.shuffle(threads)

        while True:
            n_finished = 0
            for thread in threads:
                if thread.is_alive():
                    thread.join()
                else:
                    n_finished += 1

            if n_finished == len(threads):
                break

        requests = self.spanner_service.requests
        self.assertEqual(n * 2, len(requests), msg=requests)

        client_id = self.database._nth_client_id
        channel_id = self.database._channel_id
        got_stream_segments, got_unary_segments = self.canonicalize_request_id_headers()

        want_unary_segments = [
            (
                "/google.spanner.v1.Spanner/BatchCreateSessions",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 1, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 3, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 5, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 7, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 9, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 11, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 13, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 15, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 17, 1),
            ),
            (
                "/google.spanner.v1.Spanner/GetSession",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 19, 1),
            ),
        ]
        assert got_unary_segments == want_unary_segments

        want_stream_segments = [
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 2, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 4, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 6, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 8, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 10, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 12, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 14, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 16, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 18, 1),
            ),
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, client_id, channel_id, 20, 1),
            ),
        ]
        assert got_stream_segments == want_stream_segments

    def test_database_run_in_transaction_retries_on_abort(self):
        counters = dict(aborted=0)
        want_failed_attempts = 2

        def select_in_txn(txn):
            results = txn.execute_sql("select 1")
            for row in results:
                _ = row

            if counters["aborted"] < want_failed_attempts:
                counters["aborted"] += 1
                raise Aborted(
                    "Thrown from ClientInterceptor for testing",
                    errors=[FauxCall(code_pb2.ABORTED)],
                )

        add_select1_result()
        if not getattr(self.database, "_interceptors", None):
            self.database._interceptors = MockServerTestBase._interceptors

        self.database.run_in_transaction(select_in_txn)

    def test_database_execute_partitioned_dml_request_id(self):
        add_select1_result()
        if not getattr(self.database, "_interceptors", None):
            self.database._interceptors = MockServerTestBase._interceptors
        _ = self.database.execute_partitioned_dml("select 1")

        requests = self.spanner_service.requests
        self.assertEqual(3, len(requests), msg=requests)
        self.assertTrue(isinstance(requests[0], BatchCreateSessionsRequest))
        self.assertTrue(isinstance(requests[1], BeginTransactionRequest))
        self.assertTrue(isinstance(requests[2], ExecuteSqlRequest))

        # Now ensure monotonicity of the received request-id segments.
        got_stream_segments, got_unary_segments = self.canonicalize_request_id_headers()
        NTH_CLIENT = self.database._nth_client_id
        CHANNEL_ID = self.database._channel_id
        want_unary_segments = [
            (
                "/google.spanner.v1.Spanner/BatchCreateSessions",
                (1, REQ_RAND_PROCESS_ID, NTH_CLIENT, CHANNEL_ID, 1, 1),
            ),
            (
                "/google.spanner.v1.Spanner/BeginTransaction",
                (1, REQ_RAND_PROCESS_ID, NTH_CLIENT, CHANNEL_ID, 2, 1),
            ),
        ]
        want_stream_segments = [
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, NTH_CLIENT, CHANNEL_ID, 3, 1),
            )
        ]

        assert got_unary_segments == want_unary_segments
        assert got_stream_segments == want_stream_segments

    def canonicalize_request_id_headers(self):
        src = self.database._x_goog_request_id_interceptor
        return src._stream_req_segments, src._unary_req_segments


class FauxCall:
    def __init__(self, code, details="FauxCall"):
        self._code = code
        self._details = details

    def initial_metadata(self):
        return {}

    def trailing_metadata(self):
        return {}

    def code(self):
        return self._code

    def details(self):
        return self._details
