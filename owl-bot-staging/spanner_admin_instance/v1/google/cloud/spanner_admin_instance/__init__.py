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

from google.cloud.spanner_admin_instance_v1.services.instance_admin.client import InstanceAdminClient
from google.cloud.spanner_admin_instance_v1.services.instance_admin.async_client import InstanceAdminAsyncClient

from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import CreateInstanceMetadata
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import CreateInstanceRequest
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import DeleteInstanceRequest
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import GetInstanceConfigRequest
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import GetInstanceRequest
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import Instance
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import InstanceConfig
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import ListInstanceConfigsRequest
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import ListInstanceConfigsResponse
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import ListInstancesRequest
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import ListInstancesResponse
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import ReplicaInfo
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import UpdateInstanceMetadata
from google.cloud.spanner_admin_instance_v1.types.spanner_instance_admin import UpdateInstanceRequest

__all__ = ('InstanceAdminClient',
    'InstanceAdminAsyncClient',
    'CreateInstanceMetadata',
    'CreateInstanceRequest',
    'DeleteInstanceRequest',
    'GetInstanceConfigRequest',
    'GetInstanceRequest',
    'Instance',
    'InstanceConfig',
    'ListInstanceConfigsRequest',
    'ListInstanceConfigsResponse',
    'ListInstancesRequest',
    'ListInstancesResponse',
    'ReplicaInfo',
    'UpdateInstanceMetadata',
    'UpdateInstanceRequest',
)
