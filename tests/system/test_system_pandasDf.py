# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import spanner_v1_mod
from google.auth.credentials import AnonymousCredentials
import pytest

# for referrence
TABLE_NAME = "testTable"
COLUMNS = ["id", "name"]
VALUES = [[1, "Alice"], [2, "Bob"]]


@pytest.fixture
def snapshot_obj():
    try:
        spanner_client = spanner_v1_mod.Client(
            project="test-project",
            client_options={"api_endpoint": "0.0.0.0:9010"},
            credentials=AnonymousCredentials(),
        )
        instance_id = "test-instance"
        instance = spanner_client.instance(instance_id)
        database_id = "test-database"
        database = instance.database(database_id)
        with database.snapshot() as snapshot:
            return snapshot

    except:
        pytest.skip("Cloud Spanner Emulator configuration is incorrect")

@pytest.mark.parametrize(("limit"), [(0), (1), (2)])
def test_df(limit, snapshot_obj):
    results = snapshot_obj.execute_sql(
        "Select * from testTable limit {limit}".format(limit=limit)
    )
    df = results.to_dataframe()
    assert len(df) == limit
