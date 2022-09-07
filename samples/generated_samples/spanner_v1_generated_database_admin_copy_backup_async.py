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
# Snippet for CopyBackup
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-spanner-admin-database


# [START spanner_v1_generated_DatabaseAdmin_CopyBackup_async]
# This snippet has been automatically generated and should be regarded as a
# code template only.
# It will require modifications to work:
# - It may require correct/in-range values for request initialization.
# - It may require specifying regional endpoints when creating the service
#   client as shown in:
#   https://googleapis.dev/python/google-api-core/latest/client_options.html
from google.cloud import spanner_admin_database_v1


async def sample_copy_backup():
    # Create a client
    client = spanner_admin_database_v1.DatabaseAdminAsyncClient()

    # Initialize request argument(s)
    request = spanner_admin_database_v1.CopyBackupRequest(
        parent="parent_value",
        backup_id="backup_id_value",
        source_backup="source_backup_value",
    )

    # Make the request
    operation = client.copy_backup(request=request)

    print("Waiting for operation to complete...")

    response = await operation.result()

    # Handle the response
    print(response)

# [END spanner_v1_generated_DatabaseAdmin_CopyBackup_async]
