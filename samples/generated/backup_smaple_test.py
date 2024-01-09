# Copyright 2024 Google Inc. All Rights Reserved.
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
import uuid

import pytest

import backup_sample


@pytest.fixture(scope="module")
def sample_name():
    return "backup"


def unique_database_id():
    """Creates a unique id for the database."""
    return f"test-db-{uuid.uuid4().hex[:10]}"


def unique_backup_id():
    """Creates a unique id for the backup."""
    return f"test-backup-{uuid.uuid4().hex[:10]}"


RESTORE_DB_ID = unique_database_id()
BACKUP_ID = unique_backup_id()
CMEK_RESTORE_DB_ID = unique_database_id()
CMEK_BACKUP_ID = unique_backup_id()
RETENTION_DATABASE_ID = unique_database_id()
RETENTION_PERIOD = "7d"
COPY_BACKUP_ID = unique_backup_id()


@pytest.mark.dependency(name="create_backup")
def test_create_backup(capsys, instance_id, sample_database):
    version_time = None
    with sample_database.snapshot() as snapshot:
        results = snapshot.execute_sql("SELECT CURRENT_TIMESTAMP()")
        version_time = list(results)[0][0]

    backup_sample.create_backup(
        instance_id,
        sample_database.database_id,
        BACKUP_ID,
        version_time,
    )
    out, _ = capsys.readouterr()
    assert BACKUP_ID in out
