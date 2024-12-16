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

from tests.mockserver_tests.mock_server_test_base import (
    MockServerTestBase,
    add_select1_result,
)
from google.cloud.spanner_v1.testing.interceptors import XGoogRequestIDHeaderInterceptor
from google.cloud.spanner_v1 import (
    BatchCreateSessionsRequest,
    ExecuteSqlRequest,
)
from google.cloud.spanner_v1.request_id_header import REQ_RAND_PROCESS_ID


class TestRequestIDHeader(MockServerTestBase):
    def tearDown(self):
        self.database._x_goog_request_id_interceptor.reset()

    def test_snapshot_read(self):
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

        # Now ensure monotonicity of the received request-id segments.
        got_stream_segments, got_unary_segments = self.canonicalize_request_id_headers()
        want_unary_segments = [
            (
                "/google.spanner.v1.Spanner/BatchCreateSessions",
                (1, REQ_RAND_PROCESS_ID, 1, 1, 1, 1),
            )
        ]
        want_stream_segments = [
            (
                "/google.spanner.v1.Spanner/ExecuteStreamingSql",
                (1, REQ_RAND_PROCESS_ID, 1, 1, 2, 1),
            )
        ]

        assert got_unary_segments == want_unary_segments
        assert got_stream_segments == want_stream_segments

    def canonicalize_request_id_headers(self):
        src = self.database._x_goog_request_id_interceptor
        return src._stream_req_segments, src._unary_req_segments
