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

import time

import pytest

from google.cloud import spanner_v1
from . import _helpers


@pytest.fixture(scope="function")
def if_create_instance():
    if not _helpers.CREATE_INSTANCE:
        pytest.skip(f"{_helpers.CREATE_INSTANCE_ENVVAR} not set in environment.")


@pytest.fixture(scope="function")
def no_create_instance():
    if _helpers.CREATE_INSTANCE:
        pytest.skip(f"{_helpers.CREATE_INSTANCE_ENVVAR} set in environment.")


@pytest.fixture(scope="function")
def if_backup_tests():
    if _helpers.SKIP_BACKUP_TESTS:
        pytest.skip(f"{_helpers.SKIP_BACKUP_TESTS_ENVVAR} set in environment.")


@pytest.fixture(scope="function")
def not_emulator():
    if _helpers.USE_EMULATOR:
        pytest.skip(f"{_helpers.USE_EMULATOR_ENVVAR} set in environment.")


@pytest.fixture(scope="session")
def spanner_client():
    if _helpers.USE_EMULATOR:
        from google.auth.credentials import AnonymousCredentials

        credentials = AnonymousCredentials()
        return spanner_v1.Client(
            project=_helpers.EMULATOR_PROJECT, credentials=credentials,
        )
    else:
        return spanner_v1.Client()  # use google.auth.default credentials


@pytest.fixture(scope="session")
def operation_timeout():
    return _helpers.SPANNER_OPERATION_TIMEOUT_IN_SECONDS


@pytest.fixture(scope="session")
def shared_instance_id():
    if _helpers.CREATE_INSTANCE:
        return f"{_helpers.unique_id('google-cloud')}"

    return _helpers.INSTANCE_ID


@pytest.fixture(scope="session")
def instance_config(spanner_client):
    if _helpers.USE_EMULATOR:
        return None

    configs = [
        config
        for config in _helpers.retry_503(spanner_client.list_instance_configs)()
        # Defend against back-end returning configs for regions we aren't
        # actually allowed to use.
        if _helpers.USE_EMULATOR or "-us-" in config.name
    ]
    if not configs:
        raise ValueError("No instance configs found.")

    yield configs[0]


@pytest.fixture(scope="session")
def existing_instances(spanner_client):
    instances = list(_helpers.retry_503(spanner_client.list_instances)())

    yield instances


@pytest.fixture(scope="session")
def shared_instance(
    spanner_client,
    operation_timeout,
    shared_instance_id,
    instance_config,
    existing_instances,  # evalutate before creating one
):
    _helpers.cleanup_old_instances(spanner_client)

    if _helpers.CREATE_INSTANCE:
        create_time = str(int(time.time()))
        labels = {"python-spanner-systests": "true", "created": create_time}

        instance = spanner_client.instance(
            shared_instance_id, instance_config.name, labels=labels
        )
        created_op = _helpers.retry_429_503(instance.create)()
        created_op.result(operation_timeout)  # block until completion

    else:  # reuse existing instance
        instance = spanner_client.instance(shared_instance_id)
        instance.reload()

    yield instance

    if _helpers.CREATE_INSTANCE:
        _helpers.retry_429_503(instance.delete)()
