# Copyright 2020 Google Inc. All Rights Reserved.
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

"""This application demonstrates how to create and restore from backups
using Cloud Spanner.

For more information, see the README.rst under /spanner.
"""

import argparse
from datetime import datetime, timedelta
import time

from google.cloud import spanner

# [START spanner_create_database_with_version_retention_period]
def create_database_with_version_retention_period(instance_id, database_id, retention_period):
  """Creates a database with a version retention period."""
  spanner_client = spanner.Client()
  instance = spanner_client.instance(instance_id)
  ddl_statements = [
    "CREATE TABLE Singers ("
    + "  SingerId   INT64 NOT NULL,"
    + "  FirstName  STRING(1024),"
    + "  LastName   STRING(1024),"
    + "  SingerInfo BYTES(MAX)"
    + ") PRIMARY KEY (SingerId)",
    "CREATE TABLE Albums ("
    + "  SingerId     INT64 NOT NULL,"
    + "  AlbumId      INT64 NOT NULL,"
    + "  AlbumTitle   STRING(MAX)"
    + ") PRIMARY KEY (SingerId, AlbumId),"
    + "  INTERLEAVE IN PARENT Singers ON DELETE CASCADE",
    "ALTER DATABASE {}"
    " SET OPTIONS (version_retention_period = '{}')".format(
        database_id, retention_period
    )
  ]
  db = instance.database(database_id, ddl_statements)
  operation = db.create()

  operation.result(30)

  db.reload()

  print("Database {} created with version retention period {} and earliest version time {}".format(
      db.database_id, db.version_retention_period(), db.earliest_version_time()
  ))

  db.drop()

# [END spanner_create_database_with_version_retention_period]
