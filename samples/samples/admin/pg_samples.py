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

from google.cloud import spanner
from google.cloud.spanner_admin_database_v1.types import spanner_database_admin

OPERATION_TIMEOUT_SECONDS = 240


def create_table_with_datatypes(instance_id, database_id):
    """Creates a table with supported datatypes."""
    # [START spanner_postgresql_create_table_with_datatypes]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    request = spanner_database_admin.UpdateDatabaseDdlRequest(
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

    request = spanner_database_admin.UpdateDatabaseDdlRequest(
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
