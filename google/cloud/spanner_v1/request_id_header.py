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

import os

REQ_ID_VERSION = 1  # The version of the x-goog-spanner-request-id spec.
REQ_ID_HEADER_KEY = "x-goog-spanner-request-id"


def generate_rand_uint64():
    b = os.urandom(8)
    return (
        b[7] & 0xFF
        | (b[6] & 0xFF) << 8
        | (b[5] & 0xFF) << 16
        | (b[4] & 0xFF) << 24
        | (b[3] & 0xFF) << 32
        | (b[2] & 0xFF) << 36
        | (b[1] & 0xFF) << 48
        | (b[0] & 0xFF) << 56
    )


REQ_RAND_PROCESS_ID = generate_rand_uint64()


def with_request_id(client_id, channel_id, nth_request, attempt, other_metadata=[]):
    req_id = f"{REQ_ID_VERSION}.{REQ_RAND_PROCESS_ID}.{client_id}.{channel_id}.{nth_request}.{attempt}"
    all_metadata = other_metadata.copy()
    all_metadata.append((REQ_ID_HEADER_KEY, req_id))
    return all_metadata
