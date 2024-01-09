#!/usr/bin/env python

# Copyright 2024 Google, Inc.
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

from google.cloud import spanner, spanner_admin_database_v1
from google.cloud.spanner_admin_instance_v1.types import spanner_instance_admin

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


# [START spanner_create_database_with_default_leader]
def create_database_with_default_leader(instance_id, database_id, default_leader):
    """Creates a database with tables with a default leader."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    request = spanner_admin_database_v1.CreateDatabaseRequest(
        parent=instance.name,
        create_statement="CREATE DATABASE {}".format(database_id),
        extra_statements=[
            """CREATE TABLE Singers (
                    SingerId     INT64 NOT NULL,
                    FirstName    STRING(1024),
                    LastName     STRING(1024),
                    SingerInfo   BYTES(MAX)
                ) PRIMARY KEY (SingerId)""",
            """CREATE TABLE Albums (
    SingerId     INT64 NOT NULL,
    AlbumId      INT64 NOT NULL,
    AlbumTitle   STRING(MAX)
) PRIMARY KEY (SingerId, AlbumId),
INTERLEAVE IN PARENT Singers ON DELETE CASCADE""",
            "ALTER DATABASE {}"
            " SET OPTIONS (default_leader = '{}')".format(database_id, default_leader),
        ],
    )
    operation = spanner_client.database_admin_api.create_database(request)

    print("Waiting for operation to complete...")
    database = operation.result(OPERATION_TIMEOUT_SECONDS)

    print(
        "Database {} created with default leader {}".format(
            database.name, database.default_leader
        )
    )


# [END spanner_create_database_with_default_leader]


def add_and_drop_database_roles(instance_id, database_id):
    """Showcases how to manage a user defined database role."""
    # [START spanner_add_and_drop_database_role]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    role_parent = "new_parent"
    role_child = "new_child"

    request = spanner_admin_database_v1.UpdateDatabaseDdlRequest(
        database=database.name,
        statements=[
            "CREATE ROLE {}".format(role_parent),
            "GRANT SELECT ON TABLE Singers TO ROLE {}".format(role_parent),
            "CREATE ROLE {}".format(role_child),
            "GRANT ROLE {} TO ROLE {}".format(role_parent, role_child),
        ],
    )
    operation = spanner_client.database_admin_api.update_database_ddl(request)

    operation.result(OPERATION_TIMEOUT_SECONDS)
    print(
        "Created roles {} and {} and granted privileges".format(role_parent, role_child)
    )

    request = spanner_admin_database_v1.UpdateDatabaseDdlRequest(
        database=database.name,
        statements=[
            "REVOKE ROLE {} FROM ROLE {}".format(role_parent, role_child),
            "DROP ROLE {}".format(role_child),
        ],
    )
    operation = spanner_client.database_admin_api.update_database_ddl(request)

    operation.result(OPERATION_TIMEOUT_SECONDS)
    print("Revoked privileges and dropped role {}".format(role_child))

    # [END spanner_add_and_drop_database_role]


# [START spanner_insert_data]
def insert_data(instance_id, database_id):
    """Inserts sample data into the given database.

    The database and table must already exist and can be created using
    `create_database`.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table="Singers",
            columns=("SingerId", "FirstName", "LastName"),
            values=[
                (1, "Marc", "Richards"),
                (2, "Catalina", "Smith"),
                (3, "Alice", "Trentor"),
                (4, "Lea", "Martin"),
                (5, "David", "Lomond"),
            ],
        )

        batch.insert(
            table="Albums",
            columns=("SingerId", "AlbumId", "AlbumTitle"),
            values=[
                (1, 1, "Total Junk"),
                (1, 2, "Go, Go, Go"),
                (2, 1, "Green"),
                (2, 2, "Forever Hold Your Peace"),
                (2, 3, "Terrified"),
            ],
        )

    print("Inserted data.")


# [END spanner_insert_data]
