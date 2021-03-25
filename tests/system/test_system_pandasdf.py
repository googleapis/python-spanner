# Copyright 2021 Google LLC
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

from google.cloud import spanner_v1
from google.auth.credentials import AnonymousCredentials
import pytest

# for referrence
TABLE_NAME = "testTable"
COLUMNS = ["id", "name"]
VALUES = [[1, "Alice"], [2, "Bob"]]


TABLE_NAME_1 = "Functional_Alltypes"
COLUMNS_1 = ["id", "bool_col", "date", "float_col", "string_col", "timestamp_col"]
VALUES_1 = [
    [1, True, "2016-02-09", 2.2, "David", "2002-02-10T15:30:00.45Z"],
    [2, False, "2016-10-10", 2.5, "Ryan", "2009-02-12T10:06:00.45Z"],
    [10, True, "2019-01-06", None, None, None],
    [12, True, "2018-02-02", 2.6, None, None],
    [None, None, None, None, None, None],
]


@pytest.fixture
def snapshot_obj():
    try:
        spanner_client = spanner_v1.Client(
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


@pytest.mark.parametrize(("value"), [2])
def test_rows_with_no_null_values(value, snapshot_obj):
    results = snapshot_obj.execute_sql(
        "Select * from Functional_Alltypes where id IS NOT NULL AND bool_col IS NOT NULL AND date IS NOT NULL and float_col IS NOT NULL and string_col IS NOT NULL and timestamp_col IS NOT NULL "
    )
    df = results.to_dataframe()
    assert len(df) == value


@pytest.mark.parametrize(("value"), [2])
def test_rows_with_one_or_more_null_values(value, snapshot_obj):
    results = snapshot_obj.execute_sql(
        "Select * from Functional_Alltypes where id IS NOT NULL AND string_col IS NULL AND timestamp_col IS NULL "
    )
    df = results.to_dataframe()
    assert len(df) == value


@pytest.mark.parametrize(("value"), [1])
def test_rows_with_all_null_values(value, snapshot_obj):
    results = snapshot_obj.execute_sql(
        "Select * from Functional_Alltypes where id IS NULL AND bool_col IS NULL AND date IS NULL and float_col IS NULL and string_col IS NULL and timestamp_col IS NULL "
    )
    df = results.to_dataframe()
    assert len(df) == value
