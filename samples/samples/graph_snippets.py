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

"""This application demonstrates how to do basic graph operations using
Cloud Spanner.

For more information, see the README.rst under /spanner.
"""

import argparse
import base64
import datetime
import decimal
import json
import logging
import time

from google.cloud import spanner
from google.cloud.spanner_admin_instance_v1.types import spanner_instance_admin
from google.cloud.spanner_v1 import DirectedReadOptions, param_types
from google.cloud.spanner_v1.data_types import JsonObject
from google.protobuf import field_mask_pb2  # type: ignore
from testdata import singer_pb2

OPERATION_TIMEOUT_SECONDS = 240


# [START spanner_create_instance]
def create_instance(instance_id):
    """Creates an instance."""
    from google.cloud.spanner_admin_instance_v1.types import \
        spanner_instance_admin

    spanner_client = spanner.Client()

    config_name = "{}/instanceConfigs/regional-us-central1".format(
        spanner_client.project_name
    )

    operation = spanner_client.instance_admin_api.create_instance(
        parent=spanner_client.project_name,
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


# [START spanner_create_database_with_property_graph]
def create_database_with_property_graph(instance_id, database_id):
    """Creates a database, tables and a property graph for sample data."""
    from google.cloud.spanner_admin_database_v1.types import \
        spanner_database_admin

    spanner_client = spanner.Client()
    database_admin_api = spanner_client.database_admin_api

    request = spanner_database_admin.CreateDatabaseRequest(
        parent=database_admin_api.instance_path(spanner_client.project, instance_id),
        create_statement=f"CREATE DATABASE `{database_id}`",
        extra_statements=[
            """CREATE TABLE Person (
            id               INT64 NOT NULL,
            name             STRING(MAX),
            gender           STRING(40),
            birthday         TIMESTAMP,
            country          STRING(MAX),
            city             STRING(MAX),
        ) PRIMARY KEY (id)""",
            """CREATE TABLE Account (
            id               INT64 NOT NULL,
            create_time      TIMESTAMP,
            is_blocked       BOOL,
            nick_name        STRING(MAX),
        ) PRIMARY KEY (id)""",
            """CREATE TABLE PersonOwnAccount (
            id               INT64 NOT NULL,
            account_id       INT64 NOT NULL,
            create_time      TIMESTAMP,
            FOREIGN KEY (account_id)
                REFERENCES Account (id)
        ) PRIMARY KEY (id, account_id),
        INTERLEAVE IN PARENT Person ON DELETE CASCADE""",
            """CREATE TABLE AccountTransferAccount (
            id               INT64 NOT NULL,
            to_id            INT64 NOT NULL,
            amount           FLOAT64,
            create_time      TIMESTAMP NOT NULL,
            order_number     STRING(MAX),
            FOREIGN KEY (to_id) REFERENCES Account (id)
        ) PRIMARY KEY (id, to_id, create_time),
        INTERLEAVE IN PARENT Account ON DELETE CASCADE""",
            """CREATE OR REPLACE PROPERTY GRAPH FinGraph
            NODE TABLES (Account, Person)
            EDGE TABLES (
                PersonOwnAccount
                    SOURCE KEY(id) REFERENCES Person(id)
                    DESTINATION KEY(account_id) REFERENCES Account(id)
                    LABEL Owns,
                AccountTransferAccount
                    SOURCE KEY(id) REFERENCES Account(id)
                    DESTINATION KEY(to_id) REFERENCES Account(id)
                    LABEL Transfers)""",
        ],
    )

    operation = database_admin_api.create_database(request=request)

    print("Waiting for operation to complete...")
    database = operation.result(OPERATION_TIMEOUT_SECONDS)

    print(
        "Created database {} on instance {}".format(
            database.name,
            database_admin_api.instance_path(spanner_client.project, instance_id),
        )
    )


# [END spanner_create_database_with_property_graph]


# [START spanner_insert_graph_data]
def insert_data(instance_id, database_id):
    """Inserts sample data into the given database.

    The database and tables must already exist and can be created using
    `create_database_with_property_graph`.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table="Person",
            columns=("id", "name", "country", "city"),
            values=[
                (1, "Izumi", "USA", "Mountain View"),
                (2, "Tal", "FR", "Paris"),
            ],
        )

        batch.insert(
            table="Account",
            columns=("id", "create_time", "is_blocked", "nick_name"),
            values=[
                (1, '2014-09-27T11:17:42.18Z', False, "Savings"),
                (2, '2008-07-11T12:30:00.45Z', False, "Checking"),
            ],
        )

    print("Inserted data.")


# [END spanner_insert_graph_data]


# [START spanner_insert_graph_data_with_dml]
def insert_data_with_dml(instance_id, database_id):
    """Inserts sample data into the given database using a DML statement."""
    
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def insert_owns(transaction):
        row_ct = transaction.execute_update(
            "INSERT INTO PersonOwnAccount (id, account_id, create_time) "
            " VALUES"
            "(1, 1, '2014-09-28T09:23:31.45Z'),"
            "(2, 2, '2008-07-12T04:31:18.16Z')"
        )

        print("{} record(s) inserted into PersonOwnAccount.".format(row_ct))

    def insert_transfers(transaction):
        row_ct = transaction.execute_update(
            "INSERT INTO AccountTransferAccount (id, to_id, amount, create_time, order_number) "
            " VALUES"
            "(1, 2, 900, '2024-06-24T10:11:31.26Z', '3LXCTB'),"
            "(2, 1, 100, '2024-07-01T12:23:28.11Z', '4MYRTQ')"
        )

        print("{} record(s) inserted into AccountTransferAccount.".format(row_ct))


    database.run_in_transaction(insert_owns)
    database.run_in_transaction(insert_transfers)


# [END spanner_insert_graph_data_with_dml]


# [START spanner_query_graph_data]
def query_data(instance_id, database_id):
    """Queries sample data from the database using GQL."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            """Graph FinGraph
            MATCH (a:Person)-[o:Owns]->()-[t:Transfers]->()<-[p:Owns]-(b:Person)
            RETURN a.name AS sender, b.name AS receiver, t.amount, t.create_time AS transfer_at"""
        )

        for row in results:
            print("sender: {}, receiver: {}, amount: {}, transfer_at: {}".format(*row))


# [END spanner_query_graph_data]


# [START spanner_with_graph_query_data_with_parameter]
def query_data_with_parameter(instance_id, database_id):
    """Queries sample data from the database using SQL with a parameter."""
    
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            """Graph FinGraph
            MATCH (a:Person)-[o:Owns]->()-[t:Transfers]->()<-[p:Owns]-(b:Person)
            WHERE t.amount > @min
            RETURN a.name AS sender, b.name AS receiver, t.amount, t.create_time AS transfer_at""",
            params={"min": 500},
            param_types={"min": spanner.param_types.INT64},
        )

        for row in results:
            print("sender: {}, receiver: {}, amount: {}, transfer_at: {}".format(*row))


# [END spanner_with_graph_query_data_with_parameter]


# [START spanner_delete_data]
def delete_data(instance_id, database_id):
    """Deletes sample data from the given database.

    The database, table, and data must already exist and can be created using
    `create_database` and `insert_data`.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Delete individual rows
    ownerships_to_delete = spanner.KeySet(keys=[[1, 1], [2, 2]])

    # Delete a range of rows where the column key is >=3 and <5
    transfers_range = spanner.KeyRange(start_closed=[1], end_open=[3])
    transfers_to_delete = spanner.KeySet(ranges=[transfers_range])

    with database.batch() as batch:
        batch.delete("PersonOwnAccount", ownerships_to_delete)
        batch.delete("AccountTransferAccount", transfers_to_delete)

    print("Deleted data.")


# [END spanner_delete_data]

# [START spanner_delete_graph_data_with_dml]
def delete_data_with_dml(instance_id, database_id):
    """Deletes sample data from the database using a DML statement."""
    
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def delete_persons(transaction):
        row_ct = transaction.execute_update(
            "DELETE FROM Person WHERE True"
        )

        print("{} record(s) deleted.".format(row_ct))

    def delete_accounts(transaction):
        row_ct = transaction.execute_update(
            "DELETE FROM Account AS a WHERE EXTRACT(YEAR FROM DATE(a.create_time)) >= 2000"
        )

        print("{} record(s) deleted.".format(row_ct))

    database.run_in_transaction(delete_accounts)
    database.run_in_transaction(delete_persons)


# [END spanner_delete_graph_data_with_dml]


if __name__ == "__main__":  # noqa: C901
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("instance_id", help="Your Cloud Spanner instance ID.")
    parser.add_argument(
        "--database-id", help="Your Cloud Spanner database ID.", default="example_db"
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("create_instance", help=create_instance.__doc__)
    subparsers.add_parser(
        "create_database_with_property_graph",
        help=create_database_with_property_graph.__doc__)
    subparsers.add_parser("insert_data", help=insert_data.__doc__)
    subparsers.add_parser("insert_data_with_dml", help=insert_data_with_dml.__doc__)
    subparsers.add_parser("query_data", help=query_data.__doc__)
    subparsers.add_parser(
        "query_data_with_parameter", help=query_data_with_parameter.__doc__
    )

    subparsers.add_parser("delete_data", help=delete_data.__doc__)
    subparsers.add_parser("delete_data_with_dml", help=delete_data_with_dml.__doc__)

    args = parser.parse_args()

    if args.command == "create_instance":
        create_instance(args.instance_id)
    elif args.command == "create_database_with_property_graph":
        create_database_with_property_graph(args.instance_id, args.database_id)
    elif args.command == "insert_data":
        insert_data(args.instance_id, args.database_id)
    elif args.command == "insert_data_with_dml":
        insert_data_with_dml(args.instance_id, args.database_id)
    elif args.command == "query_data":
        query_data(args.instance_id, args.database_id)
    elif args.command == "query_data_with_parameter":
        query_data_with_parameter(args.instance_id, args.database_id)
    elif args.command == "delete_data":
        delete_data(args.instance_id, args.database_id)
    elif args.command == "delete_data_with_dml":
        delete_data_with_dml(args.instance_id, args.database_id)

