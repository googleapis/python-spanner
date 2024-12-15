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

class TestRequestIDHeader(MockServerTestBase):
    # Firstly inject in the XGoogRequestIdHeader interceptor.
    x_goog_request_id_interceptor = XGoogRequestIDHeaderInterceptor()
    MockServerTestBase._interceptors = [x_goog_request_id_interceptor]

    def tearDown(self):
        x_goog_request_id_interceptor.reset()

    def test_snapshot_read(self):
        add_select1_result()
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql("select 1")
            result_list = []
            for result in results:
                result_list.append(result)
                self.assertEqual(1, row[0])
            self.assertEqual(1, len(result_list))

        requests = self.spanner_service.requests
        self.assertEqual(2, len(requests), msg=requests)
        self.assertTrue(isinstance(requests[0], BatchCreateSessionsRequest))
        self.assertTrue(isinstance(requests[1], ExecuteSqlRequest))

        # Now ensure monotonicity of the received request-id segments.
        stream_segments, unary_segments = self.canonicalize_request_id_headers()
        assert len(unary_segments) > 1
        assert len(stream_segments) == 0

    def canonicalize_request_id_headers(self):
        src = x_goog_request_id_interceptor
        stream_segments = [
            parse_request_id(req_id) for req_id in src._stream_req_segments
        ]
        unary_segments = [
            parse_request_id(req_id) for req_id in src._unary_req_segments
        ]
        return stream_segments, unary_segments


def parse_request_id(request_id_str):
    splits = request_id_str.split(".")
    version, rand_process_id, client_id, channel_id, nth_request, nth_attempt = list(
        map(lambda v: int(v), splits)
    )
    return (
        version,
        rand_process_id,
        client_id,
        channel_id,
        nth_request,
        nth_attempt,
    )
