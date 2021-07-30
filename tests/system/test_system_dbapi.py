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
import unittest

from google.api_core import exceptions

from google.cloud.spanner_v1 import BurstyPool
from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.instance import Backup
from google.cloud.spanner_v1.instance import Instance

from test_utils.retry import RetryErrors

from .test_system import (
    CREATE_INSTANCE,
    EXISTING_INSTANCES,
    INSTANCE_ID,
    USE_EMULATOR,
    _list_instances,
    Config,
)


SPANNER_OPERATION_TIMEOUT_IN_SECONDS = int(
    os.getenv("SPANNER_OPERATION_TIMEOUT_IN_SECONDS", 60)
)


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
    for instance_pb in Config.CLIENT.list_instances(
        "labels.python-spanner-dbapi-systests:true"
    ):
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
        labels = {"python-spanner-dbapi-systests": "true", "created": create_time}

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


class TestTransactionsManagement(unittest.TestCase):
    """Transactions management support tests."""

    DATABASE_NAME = "db-api-transactions-management"

    DDL_STATEMENTS = (
        """CREATE TABLE contacts (
            contact_id INT64,
            first_name STRING(1024),
            last_name STRING(1024),
            email STRING(1024)
        )
        PRIMARY KEY (contact_id)""",
    )

    @classmethod
    def setUpClass(cls):
        """Create a test database."""
        cls._db = Config.INSTANCE.database(
            cls.DATABASE_NAME,
            ddl_statements=cls.DDL_STATEMENTS,
            pool=BurstyPool(labels={"testcase": "database_api"}),
        )
        cls._db.create().result(
            SPANNER_OPERATION_TIMEOUT_IN_SECONDS
        )  # raises on failure / timeout.

    @classmethod
    def tearDownClass(cls):
        """Delete the test database."""
        cls._db.drop()

    def tearDown(self):
        """Clear the test table after every test."""
        self._db.run_in_transaction(clear_table)


def clear_table(transaction):
    """Clear the test table."""
    transaction.execute_update("DELETE FROM contacts WHERE true")
