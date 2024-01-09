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
Spanner PostgreSql dialect.

For more information, see the README.rst under /spanner.
"""
import base64
import decimal

from google.cloud import spanner, spanner_admin_database_v1

OPERATION_TIMEOUT_SECONDS = 240


def create_table_with_datatypes(instance_id, database_id):
    """Creates a table with supported datatypes."""
    # [START spanner_postgresql_create_table_with_datatypes]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    request = spanner_admin_database_v1.UpdateDatabaseDdlRequest(
        database=database.name,
        statements=[
            """CREATE TABLE Venues (
  VenueId         BIGINT NOT NULL,
  VenueName       character varying(100),
  VenueInfo       BYTEA,
  Capacity        BIGINT,
  OutdoorVenue    BOOL,
  PopularityScore FLOAT8,
  Revenue         NUMERIC,
  LastUpdateTime  SPANNER.COMMIT_TIMESTAMP NOT NULL,
  PRIMARY KEY (VenueId))"""
        ],
    )
    operation = spanner_client.database_admin_api.update_database_ddl(request)

    print("Waiting for operation to complete...")
    operation.result(OPERATION_TIMEOUT_SECONDS)

    print(
        "Created Venues table on database {} on instance {}".format(
            database_id, instance_id
        )
    )
    # [END spanner_postgresql_create_table_with_datatypes]


# [START spanner_postgresql_jsonb_add_column]
def add_jsonb_column(instance_id, database_id):
    """
    Alters Venues tables in the database adding a JSONB column.
    You can create the table by running the `create_table_with_datatypes`
    sample or by running this DDL statement against your database:
    CREATE TABLE Venues (
      VenueId         BIGINT NOT NULL,
      VenueName       character varying(100),
      VenueInfo       BYTEA,
      Capacity        BIGINT,
      OutdoorVenue    BOOL,
      PopularityScore FLOAT8,
      Revenue         NUMERIC,
      LastUpdateTime  SPANNER.COMMIT_TIMESTAMP NOT NULL,
      PRIMARY KEY (VenueId))
    """
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    request = spanner_admin_database_v1.UpdateDatabaseDdlRequest(
        database=database.name,
        statements=["ALTER TABLE Venues ADD COLUMN VenueDetails JSONB"],
    )

    operation = spanner_client.database_admin_api.update_database_ddl(request)

    print("Waiting for operation to complete...")
    operation.result(OPERATION_TIMEOUT_SECONDS)

    print(
        'Altered table "Venues" on database {} on instance {}.'.format(
            database_id, instance_id
        )
    )


# [END spanner_postgresql_jsonb_add_column]


def insert_datatypes_data(instance_id, database_id):
    """Inserts data with supported datatypes into a table."""
    # [START spanner_postgresql_insert_datatypes_data]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    exampleBytes1 = base64.b64encode("Hello World 1".encode())
    exampleBytes2 = base64.b64encode("Hello World 2".encode())
    exampleBytes3 = base64.b64encode("Hello World 3".encode())
    with database.batch() as batch:
        batch.insert(
            table="Venues",
            columns=(
                "VenueId",
                "VenueName",
                "VenueInfo",
                "Capacity",
                "OutdoorVenue",
                "PopularityScore",
                "Revenue",
                "LastUpdateTime",
            ),
            values=[
                (
                    4,
                    "Venue 4",
                    exampleBytes1,
                    1800,
                    False,
                    0.85543,
                    decimal.Decimal("215100.10"),
                    spanner.COMMIT_TIMESTAMP,
                ),
                (
                    19,
                    "Venue 19",
                    exampleBytes2,
                    6300,
                    True,
                    0.98716,
                    decimal.Decimal("1200100.00"),
                    spanner.COMMIT_TIMESTAMP,
                ),
                (
                    42,
                    "Venue 42",
                    exampleBytes3,
                    3000,
                    False,
                    0.72598,
                    decimal.Decimal("390650.99"),
                    spanner.COMMIT_TIMESTAMP,
                ),
            ],
        )

    print("Inserted data.")
    # [END spanner_postgresql_insert_datatypes_data]
