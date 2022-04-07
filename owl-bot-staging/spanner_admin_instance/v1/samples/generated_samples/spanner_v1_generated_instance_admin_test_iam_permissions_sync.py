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
# Snippet for TestIamPermissions
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-spanner-admin-instance


# [START spanner_v1_generated_InstanceAdmin_TestIamPermissions_sync]
from google.cloud import spanner_admin_instance_v1


def sample_test_iam_permissions():
    # Create a client
    client = spanner_admin_instance_v1.InstanceAdminClient()

    # Initialize request argument(s)
    request = spanner_admin_instance_v1.TestIamPermissionsRequest(
        resource="resource_value",
        permissions=['permissions_value_1', 'permissions_value_2'],
    )

    # Make the request
    response = client.test_iam_permissions(request=request)

    # Handle the response
    print(response)

# [END spanner_v1_generated_InstanceAdmin_TestIamPermissions_sync]
