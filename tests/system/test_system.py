# Copyright 2016 Google LLC All rights reserved.
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

from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.instance import Backup
from google.cloud.spanner_v1.instance import Instance

from test_utils.retry import RetryErrors
from test_utils.system import unique_resource_id

CREATE_INSTANCE = os.getenv("GOOGLE_CLOUD_TESTS_CREATE_SPANNER_INSTANCE") is not None
USE_EMULATOR = os.getenv("SPANNER_EMULATOR_HOST") is not None
SPANNER_OPERATION_TIMEOUT_IN_SECONDS = int(
    os.getenv("SPANNER_OPERATION_TIMEOUT_IN_SECONDS", 60)
)

if CREATE_INSTANCE:
    INSTANCE_ID = "google-cloud" + unique_resource_id("-")
else:
    INSTANCE_ID = os.environ.get(
        "GOOGLE_CLOUD_TESTS_SPANNER_INSTANCE", "google-cloud-python-systest"
    )
EXISTING_INSTANCES = []


class Config(object):
    """Run-time configuration to be modified at set-up.

    This is a mutable stand-in to allow test set-up to modify
    global state.
    """

    CLIENT = None
    INSTANCE_CONFIG = None
    INSTANCE = None


def _list_instances():
    return list(Config.CLIENT.list_instances())


def setUpModule():
    if USE_EMULATOR:
        from google.auth.credentials import AnonymousCredentials

        emulator_project = os.getenv("GCLOUD_PROJECT", "emulator-test-project")
        Config.CLIENT = Client(
            project=emulator_project, credentials=AnonymousCredentials()
        )
    else:
        Config.CLIENT = Client()
    retry = RetryErrors(exceptions.ServiceUnavailable)

    configs = list(retry(Config.CLIENT.list_instance_configs)())

    instances = retry(_list_instances)()
    EXISTING_INSTANCES[:] = instances

    # Delete test instances that are older than an hour.
    cutoff = int(time.time()) - 1 * 60 * 60
    instance_pbs = Config.CLIENT.list_instances("labels.python-spanner-systests:true")
    for instance_pb in instance_pbs:
        instance = Instance.from_pb(instance_pb, Config.CLIENT)
        if "created" not in instance.labels:
            continue
        create_time = int(instance.labels["created"])
        if create_time > cutoff:
            continue
        # Instance cannot be deleted while backups exist.
        for backup_pb in instance.list_backups():
            backup = Backup.from_pb(backup_pb, instance)
            backup.delete()
        instance.delete()

    if CREATE_INSTANCE:
        if not USE_EMULATOR:
            # Defend against back-end returning configs for regions we aren't
            # actually allowed to use.
            configs = [config for config in configs if "-us-" in config.name]

        if not configs:
            raise ValueError("List instance configs failed in module set up.")

        Config.INSTANCE_CONFIG = configs[0]
        config_name = configs[0].name
        create_time = str(int(time.time()))
        labels = {"python-spanner-systests": "true", "created": create_time}

        Config.INSTANCE = Config.CLIENT.instance(
            INSTANCE_ID, config_name, labels=labels
        )
        created_op = Config.INSTANCE.create()
        created_op.result(
            SPANNER_OPERATION_TIMEOUT_IN_SECONDS
        )  # block until completion

    else:
        Config.INSTANCE = Config.CLIENT.instance(INSTANCE_ID)
        Config.INSTANCE.reload()


def tearDownModule():
    if CREATE_INSTANCE:
        Config.INSTANCE.delete()
