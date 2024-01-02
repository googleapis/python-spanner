#!/usr/bin/env python

# Copyright 2016 Google, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This application demonstrates how to do basic operations using Cloud
Spanner.

For more information, see the README.rst under /spanner.
"""

import time

from google.cloud import spanner
from google.cloud.spanner_admin_instance_v1.types import spanner_instance_admin
from google.cloud.spanner_v1 import param_types
from google.type import expr_pb2
from google.iam.v1 import policy_pb2
from google.cloud.spanner_v1.data_types import JsonObject
from google.protobuf import field_mask_pb2  # type: ignore

OPERATION_TIMEOUT_SECONDS = 240


# [START spanner_create_instance]
def create_instance(instance_id):
    """Creates an instance."""
    spanner_client = spanner.Client()

    config_name = "{}/instanceConfigs/regional-us-central1".format(
        spanner_client.project_name
    )

    operation = spanner_client.instance_admin_api.create_instance(
        parent="projects/{}".format(spanner_client.project),
        instance_id=instance_id,
        instance=spanner_instance_admin.Instance(
            config=config_name,
            display_name="This is a display name.",
            node_count=1,
            labels={
                "cloud_spanner_samples": "true",
                "sample_name": "snippets-create_instance-explicit",
                "created": str(int(time.time())),
            },
        ),
    )

    print("Waiting for operation to complete...")
    operation.result(OPERATION_TIMEOUT_SECONDS)

    print("Created instance {}".format(instance_id))


# [END spanner_create_instance]
