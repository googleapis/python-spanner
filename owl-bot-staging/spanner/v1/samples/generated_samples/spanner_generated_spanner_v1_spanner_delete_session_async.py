# -*- coding: utf-8 -*-
# Copyright 2020 Google LLC
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
#
# Generated code. DO NOT EDIT!
#
# Snippet for DeleteSession
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-spanner


# [START spanner_generated_spanner_v1_Spanner_DeleteSession_async]
from google.cloud import spanner_v1


async def sample_delete_session():
    # Create a client
    client = spanner_v1.SpannerAsyncClient()

    # Initialize request argument(s)
    request = spanner_v1.DeleteSessionRequest(
        name="name_value",
    )

    # Make the request
    await client.delete_session(request=request)


# [END spanner_generated_spanner_v1_Spanner_DeleteSession_async]
