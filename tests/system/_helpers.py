# Copyright 2021 Google LLC All rights reserved.
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
import time

from google.api_core import exceptions
from google.cloud.spanner_v1 import instance as instance_mod
from test_utils import retry
from test_utils import system


CREATE_INSTANCE_ENVVAR = "GOOGLE_CLOUD_TESTS_CREATE_SPANNER_INSTANCE"
CREATE_INSTANCE = os.getenv(CREATE_INSTANCE_ENVVAR) is not None

INSTANCE_ID_ENVVAR = "GOOGLE_CLOUD_TESTS_SPANNER_INSTANCE"
INSTANCE_ID_DEFAULT = "google-cloud-python-systest"
INSTANCE_ID = os.environ.get(INSTANCE_ID_ENVVAR, INSTANCE_ID_DEFAULT)

SKIP_BACKUP_TESTS_ENVVAR = "SKIP_BACKUP_TESTS"
SKIP_BACKUP_TESTS = os.getenv(SKIP_BACKUP_TESTS_ENVVAR) is not None

SPANNER_OPERATION_TIMEOUT_IN_SECONDS = int(
    os.getenv("SPANNER_OPERATION_TIMEOUT_IN_SECONDS", 60)
)

USE_EMULATOR_ENVVAR = "SPANNER_EMULATOR_HOST"
USE_EMULATOR = os.getenv(USE_EMULATOR_ENVVAR) is not None

EMULATOR_PROJECT_ENVVAR = "GCLOUD_PROJECT"
EMULATOR_PROJECT_DEFAULT = "emulator-test-project"
EMULATOR_PROJECT = os.getenv(EMULATOR_PROJECT_ENVVAR, EMULATOR_PROJECT_DEFAULT)


retry_503 = retry.RetryErrors(exceptions.ServiceUnavailable)
retry_429_503 = retry.RetryErrors(
    exceptions.TooManyRequests, exceptions.ServiceUnavailable,
)


def scrub_instance_ignore_not_found(to_scrub):
    """Helper for func:`cleanup_old_instances`"""
    try:
        for backup_pb in to_scrub.list_backups():
            # Instance cannot be deleted while backups exist.
            bkp = instance_mod.Backup.from_pb(backup_pb, to_scrub)
            retry_429_503(bkp.delete)()

        retry_429_503(to_scrub.delete)()
    except exceptions.NotFound:  # lost the race
        pass


def cleanup_old_instances(spanner_client):
    cutoff = int(time.time()) - 1 * 60 * 60  # one hour ago
    instance_filter = "labels.python-spanner-systests:true"

    for instance_pb in spanner_client.list_instances(filter_=instance_filter):
        instance = instance_mod.Instance.from_pb(instance_pb, spanner_client)

        if "created" in instance.labels:
            create_time = int(instance.labels["created"])

            if create_time <= cutoff:
                scrub_instance_ignore_not_found(instance)


def unique_id(prefix):
    return f"{prefix}{system.unique_resource_id('-')}"
