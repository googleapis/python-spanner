# Copyright 2016 Google, Inc.
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

import time
import uuid

from google.cloud import spanner
import pytest

import snippets


def unique_instance_id():
    """ Creates a unique id for the database. """
    return f'test-instance-{uuid.uuid4().hex[:10]}'


def unique_database_id():
    """ Creates a unique id for the database. """
    return f'test-db-{uuid.uuid4().hex[:10]}'


INSTANCE_ID = unique_instance_id()
DATABASE_ID = unique_database_id()


@pytest.fixture(scope='module')
def spanner_instance():
    snippets.create_instance(INSTANCE_ID)
    spanner_client = spanner.Client()
    instance = spanner_client.instance(INSTANCE_ID)
    yield instance
    instance.delete()


@pytest.fixture(scope='module')
def database(spanner_instance):
    """ Creates a temporary database that is removed after testing. """
    snippets.create_database(INSTANCE_ID, DATABASE_ID)
    db = spanner_instance.database(DATABASE_ID)
    yield db
    db.drop()


def test_create_instance(spanner_instance):
    # Reload will only succeed if the instance exists.
    spanner_instance.reload()


def test_create_database(database):
    # Reload will only succeed if the database exists.
    database.reload()


def test_insert_data(capsys):
    snippets.insert_data(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Inserted data' in out


def test_delete_data(capsys):
    snippets.delete_data(INSTANCE_ID, DATABASE_ID)
    snippets.insert_data(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Deleted data' in out


def test_query_data(capsys):
    snippets.query_data(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 1, AlbumId: 1, AlbumTitle: Total Junk' in out


def test_add_column(capsys):
    snippets.add_column(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Added the MarketingBudget column.' in out


def test_read_data(capsys):
    snippets.read_data(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 1, AlbumId: 1, AlbumTitle: Total Junk' in out


def test_update_data(capsys):
    # Sleep for 15 seconds to ensure previous inserts will be
    # 'stale' by the time test_read_stale_data is run.
    time.sleep(15)

    snippets.update_data(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Updated data.' in out


def test_read_stale_data(capsys):
    # This snippet relies on test_update_data inserting data
    # at least 15 seconds after the previous insert
    snippets.read_stale_data(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 1, AlbumId: 1, MarketingBudget: None' in out


def test_read_write_transaction(capsys):
    snippets.read_write_transaction(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Transaction complete' in out


def test_query_data_with_new_column(capsys):
    snippets.query_data_with_new_column(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 1, AlbumId: 1, MarketingBudget: 300000' in out
    assert 'SingerId: 2, AlbumId: 2, MarketingBudget: 300000' in out


def test_add_index(capsys):
    snippets.add_index(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Added the AlbumsByAlbumTitle index' in out


def test_query_data_with_index(capsys):
    snippets.query_data_with_index(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Go, Go, Go' in out
    assert 'Forever Hold Your Peace' in out
    assert 'Green' not in out


def test_read_data_with_index(capsys):
    snippets.read_data_with_index(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Go, Go, Go' in out
    assert 'Forever Hold Your Peace' in out
    assert 'Green' in out


def test_add_storing_index(capsys):
    snippets.add_storing_index(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Added the AlbumsByAlbumTitle2 index.' in out


def test_read_data_with_storing_index(capsys):
    snippets.read_data_with_storing_index(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert '300000' in out


def test_read_only_transaction(capsys):
    snippets.read_only_transaction(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    # Snippet does two reads, so entry should be listed twice
    assert out.count('SingerId: 1, AlbumId: 1, AlbumTitle: Total Junk') == 2


def test_add_timestamp_column(capsys):
    snippets.add_timestamp_column(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Altered table "Albums" on database ' in out


def test_update_data_with_timestamp(capsys):
    snippets.update_data_with_timestamp(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Updated data' in out


def test_query_data_with_timestamp(capsys):
    snippets.query_data_with_timestamp(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 1, AlbumId: 1, MarketingBudget: 1000000' in out
    assert 'SingerId: 2, AlbumId: 2, MarketingBudget: 750000' in out


def test_create_table_with_timestamp(capsys):
    snippets.create_table_with_timestamp(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Created Performances table on database' in out


def test_insert_data_with_timestamp(capsys):
    snippets.insert_data_with_timestamp(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Inserted data.' in out


def test_write_struct_data(capsys):
    snippets.write_struct_data(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Inserted sample data for STRUCT queries' in out


def test_query_with_struct(capsys):
    snippets.query_with_struct(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 6' in out


def test_query_with_array_of_struct(capsys):
    snippets.query_with_array_of_struct(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 8' in out
    assert 'SingerId: 7' in out
    assert 'SingerId: 6' in out


def test_query_struct_field(capsys):
    snippets.query_struct_field(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 6' in out


def test_query_nested_struct_field(capsys):
    snippets.query_nested_struct_field(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 6 SongName: Imagination' in out
    assert 'SingerId: 9 SongName: Imagination' in out


def test_insert_data_with_dml(capsys):
    snippets.insert_data_with_dml(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert '1 record(s) inserted.' in out


def test_update_data_with_dml(capsys):
    snippets.update_data_with_dml(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert '1 record(s) updated.' in out


def test_delete_data_with_dml(capsys):
    snippets.delete_data_with_dml(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert '1 record(s) deleted.' in out


def test_update_data_with_dml_timestamp(capsys):
    snippets.update_data_with_dml_timestamp(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert '2 record(s) updated.' in out


def test_dml_write_read_transaction(capsys):
    snippets.dml_write_read_transaction(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert '1 record(s) inserted.' in out
    assert 'FirstName: Timothy, LastName: Campbell' in out


def test_update_data_with_dml_struct(capsys):
    snippets.update_data_with_dml_struct(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert '1 record(s) updated' in out


def test_insert_with_dml(capsys):
    snippets.insert_with_dml(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert '4 record(s) inserted' in out


def test_query_data_with_parameter(capsys):
    snippets.query_data_with_parameter(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'SingerId: 12, FirstName: Melissa, LastName: Garcia' in out


def test_write_with_dml_transaction(capsys):
    snippets.write_with_dml_transaction(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert "Transferred 200000 from Album2's budget to Album1's" in out


def update_data_with_partitioned_dml(capsys):
    snippets.update_data_with_partitioned_dml(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert "3 record(s) updated" in out


def delete_data_with_partitioned_dml(capsys):
    snippets.delete_data_with_partitioned_dml(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert "5 record(s) deleted" in out


def update_with_batch_dml(capsys):
    snippets.update_with_batch_dml(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert "Executed 2 SQL statements using Batch DML" in out


def test_create_table_with_datatypes(capsys):
    snippets.create_table_with_datatypes(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Created Venues table on database' in out


def test_insert_datatypes_data(capsys):
    snippets.insert_datatypes_data(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'Inserted data.' in out


def test_query_data_with_array(capsys):
    snippets.query_data_with_array(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 19, VenueName: Venue 19, AvailableDate: 2020-11-01' in out
    assert 'VenueId: 42, VenueName: Venue 42, AvailableDate: 2020-10-01' in out


def test_query_data_with_bool(capsys):
    snippets.query_data_with_bool(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 19, VenueName: Venue 19, OutdoorVenue: True' in out


def test_query_data_with_bytes(capsys):
    snippets.query_data_with_bytes(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 4, VenueName: Venue 4' in out


def test_query_data_with_date(capsys):
    snippets.query_data_with_date(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 4, VenueName: Venue 4, LastContactDate: 2018-09-02' in out
    assert 'VenueId: 42, VenueName: Venue 42, LastContactDate: 2018-10-01' \
        in out


def test_query_data_with_float(capsys):
    snippets.query_data_with_float(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 4, VenueName: Venue 4, PopularityScore: 0.8' in out
    assert 'VenueId: 19, VenueName: Venue 19, PopularityScore: 0.9' in out


def test_query_data_with_int(capsys):
    snippets.query_data_with_int(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 19, VenueName: Venue 19, Capacity: 6300' in out
    assert 'VenueId: 42, VenueName: Venue 42, Capacity: 3000' in out


def test_query_data_with_string(capsys):
    snippets.query_data_with_string(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 42, VenueName: Venue 42' in out


def test_query_data_with_timestamp_parameter(capsys):
    # Wait 5 seconds to avoid a time drift issue for the next query:
    # https://github.com/GoogleCloudPlatform/python-docs-samples/issues/4197.
    time.sleep(5)
    snippets.query_data_with_timestamp_parameter(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 4, VenueName: Venue 4, LastUpdateTime:' in out
    assert 'VenueId: 19, VenueName: Venue 19, LastUpdateTime:' in out
    assert 'VenueId: 42, VenueName: Venue 42, LastUpdateTime:' in out


def test_query_data_with_query_options(capsys):
    snippets.query_data_with_query_options(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 4, VenueName: Venue 4, LastUpdateTime:' in out
    assert 'VenueId: 19, VenueName: Venue 19, LastUpdateTime:' in out
    assert 'VenueId: 42, VenueName: Venue 42, LastUpdateTime:' in out


def test_create_client_with_query_options(capsys):
    snippets.create_client_with_query_options(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert 'VenueId: 4, VenueName: Venue 4, LastUpdateTime:' in out
    assert 'VenueId: 19, VenueName: Venue 19, LastUpdateTime:' in out
    assert 'VenueId: 42, VenueName: Venue 42, LastUpdateTime:' in out
