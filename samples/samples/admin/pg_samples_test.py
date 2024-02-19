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

import uuid

import pg_samples as samples
import pytest
from google.api_core import exceptions
from google.cloud.spanner_admin_database_v1.types.common import DatabaseDialect
from test_utils.retry import RetryErrors

CREATE_TABLE_SINGERS = """\
CREATE TABLE Singers (
    SingerId     BIGINT NOT NULL,
    FirstName    CHARACTER VARYING(1024),
    LastName     CHARACTER VARYING(1024),
    SingerInfo   BYTEA,
    FullName     CHARACTER VARYING(2048)
        GENERATED ALWAYS AS (FirstName || ' ' || LastName) STORED,
    PRIMARY KEY (SingerId)
)
"""

CREATE_TABLE_ALBUMS = """\
CREATE TABLE Albums (
    SingerId     BIGINT NOT NULL,
    AlbumId      BIGINT NOT NULL,
    AlbumTitle   CHARACTER VARYING(1024),
    PRIMARY KEY (SingerId, AlbumId)
    ) INTERLEAVE IN PARENT Singers ON DELETE CASCADE
"""

retry_429 = RetryErrors(exceptions.ResourceExhausted, delay=15)


@pytest.fixture(scope="module")
def sample_name():
    return "pg_snippets"


@pytest.fixture(scope="module")
def database_dialect():
    """Spanner dialect to be used for this sample.
    The dialect is used to initialize the dialect for the database.
    It can either be GoogleStandardSql or PostgreSql.
    """
    return DatabaseDialect.POSTGRESQL


@pytest.fixture(scope="module")
def create_instance_id():
    """Id for the low-cost instance."""
    return f"create-instance-{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module")
def lci_instance_id():
    """Id for the low-cost instance."""
    return f"lci-instance-{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module")
def database_id():
    return f"test-db-{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module")
def create_database_id():
    return f"create-db-{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module")
def cmek_database_id():
    return f"cmek-db-{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module")
def default_leader_database_id():
    return f"leader_db_{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module")
def database_ddl():
    """Sequence of DDL statements used to set up the database.
    Sample testcase modules can override as needed.
    """
    return [CREATE_TABLE_SINGERS, CREATE_TABLE_ALBUMS]


@pytest.fixture(scope="module")
def default_leader():
    """Default leader for multi-region instances."""
    return "us-east4"


@pytest.mark.dependency(name="create_table_with_datatypes")
def test_create_table_with_datatypes(capsys, instance_id, sample_database):
    samples.create_table_with_datatypes(instance_id, sample_database.database_id)
    out, _ = capsys.readouterr()
    assert "Created Venues table on database" in out


@pytest.mark.dependency(name="add_jsonb_column", depends=["insert_datatypes_data"])
def test_add_jsonb_column(capsys, instance_id, sample_database):
    samples.add_jsonb_column(instance_id, sample_database.database_id)
    out, _ = capsys.readouterr()
    assert "Waiting for operation to complete..." in out
    assert 'Altered table "Venues" on database ' in out
