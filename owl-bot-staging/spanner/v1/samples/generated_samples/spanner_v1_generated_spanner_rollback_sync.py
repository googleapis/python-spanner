# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
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
# Snippet for Rollback
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-spanner


# [START spanner_v1_generated_Spanner_Rollback_sync]
from google.cloud import spanner_v1


def sample_rollback():
    # Create a client
    client = spanner_v1.SpannerClient()

    # Initialize request argument(s)
    request = spanner_v1.RollbackRequest(
        session="session_value",
        transaction_id=b'transaction_id_blob',
    )

    # Make the request
    client.rollback(request=request)


# [END spanner_v1_generated_Spanner_Rollback_sync]
