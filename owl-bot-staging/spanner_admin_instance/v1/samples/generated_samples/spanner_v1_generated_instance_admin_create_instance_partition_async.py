# -*- coding: utf-8 -*-
# Copyright 2024 Google LLC
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
# Snippet for CreateInstancePartition
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-spanner-admin-instance


# [START spanner_v1_generated_InstanceAdmin_CreateInstancePartition_async]
# This snippet has been automatically generated and should be regarded as a
# code template only.
# It will require modifications to work:
# - It may require correct/in-range values for request initialization.
# - It may require specifying regional endpoints when creating the service
#   client as shown in:
#   https://googleapis.dev/python/google-api-core/latest/client_options.html
from google.cloud import spanner_admin_instance_v1


async def sample_create_instance_partition():
    # Create a client
    client = spanner_admin_instance_v1.InstanceAdminAsyncClient()

    # Initialize request argument(s)
    instance_partition = spanner_admin_instance_v1.InstancePartition()
    instance_partition.node_count = 1070
    instance_partition.name = "name_value"
    instance_partition.config = "config_value"
    instance_partition.display_name = "display_name_value"

    request = spanner_admin_instance_v1.CreateInstancePartitionRequest(
        parent="parent_value",
        instance_partition_id="instance_partition_id_value",
        instance_partition=instance_partition,
    )

    # Make the request
    operation = client.create_instance_partition(request=request)

    print("Waiting for operation to complete...")

    response = (await operation).result()

    # Handle the response
    print(response)

# [END spanner_v1_generated_InstanceAdmin_CreateInstancePartition_async]
