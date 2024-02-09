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

import uuid

from google.api_core import exceptions
from google.cloud.spanner_admin_database_v1.types.common import DatabaseDialect
import pytest
from test_utils.retry import RetryErrors

import samples

CREATE_TABLE_SINGERS = """\
CREATE TABLE Singers (
    SingerId     INT64 NOT NULL,
    FirstName    STRING(1024),
    LastName     STRING(1024),
    SingerInfo   BYTES(MAX),
    FullName     STRING(2048) AS (
        ARRAY_TO_STRING([FirstName, LastName], " ")
    ) STORED
) PRIMARY KEY (SingerId)
"""

CREATE_TABLE_ALBUMS = """\
CREATE TABLE Albums (
    SingerId     INT64 NOT NULL,
    AlbumId      INT64 NOT NULL,
    AlbumTitle   STRING(MAX)
) PRIMARY KEY (SingerId, AlbumId),
INTERLEAVE IN PARENT Singers ON DELETE CASCADE
"""

retry_429 = RetryErrors(exceptions.ResourceExhausted, delay=15)


@pytest.fixture(scope="module")
def sample_name():
    return "snippets"


@pytest.fixture(scope="module")
def database_dialect():
    """Spanner dialect to be used for this sample.

    The dialect is used to initialize the dialect for the database.
    It can either be GoogleStandardSql or PostgreSql.
    """
    return DatabaseDialect.GOOGLE_STANDARD_SQL


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


@pytest.fixture(scope="module")
def base_instance_config_id(spanner_client):
    return "{}/instanceConfigs/{}".format(spanner_client.project_name, "nam7")


def test_create_instance_explicit(spanner_client, create_instance_id):
    # Rather than re-use 'sample_isntance', we create a new instance, to
    # ensure that the 'create_instance' snippet is tested.
    retry_429(samples.create_instance)(create_instance_id)
    instance = spanner_client.instance(create_instance_id)
    retry_429(instance.delete)()


def test_create_database_with_default_leader(
    capsys,
    multi_region_instance,
    multi_region_instance_id,
    default_leader_database_id,
    default_leader,
):
    retry_429 = RetryErrors(exceptions.ResourceExhausted, delay=15)
    retry_429(samples.create_database_with_default_leader)(
        multi_region_instance_id, default_leader_database_id, default_leader
    )
    out, _ = capsys.readouterr()
    assert default_leader_database_id in out
    assert default_leader in out


@pytest.mark.dependency(name="add_and_drop_database_roles")
def test_add_and_drop_database_roles(capsys, instance_id, sample_database):
    samples.add_and_drop_database_roles(instance_id, sample_database.database_id)
    out, _ = capsys.readouterr()
    assert "Created roles new_parent and new_child and granted privileges" in out
    assert "Revoked privileges and dropped role new_child" in out


@pytest.mark.dependency(name="create_table_with_datatypes")
def test_create_table_with_datatypes(capsys, instance_id, sample_database):
    samples.create_table_with_datatypes(instance_id, sample_database.database_id)
    out, _ = capsys.readouterr()
    assert "Created Venues table on database" in out


@pytest.mark.dependency(
    name="add_json_column",
    depends=["create_table_with_datatypes"],
)
def test_add_json_column(capsys, instance_id, sample_database):
    samples.add_json_column(instance_id, sample_database.database_id)
    out, _ = capsys.readouterr()
    assert 'Altered table "Venues" on database ' in out


@pytest.mark.dependency(
    name="add_numeric_column",
    depends=["create_table_with_datatypes"],
)
def test_add_numeric_column(capsys, instance_id, sample_database):
    samples.add_numeric_column(instance_id, sample_database.database_id)
    out, _ = capsys.readouterr()
    assert 'Altered table "Venues" on database ' in out


@pytest.mark.dependency(name="create_table_with_foreign_key_delete_cascade")
def test_create_table_with_foreign_key_delete_cascade(
    capsys, instance_id, sample_database
):
    samples.create_table_with_foreign_key_delete_cascade(
        instance_id, sample_database.database_id
    )
    out, _ = capsys.readouterr()
    assert (
        "Created Customers and ShoppingCarts table with FKShoppingCartsCustomerId"
        in out
    )


@pytest.mark.dependency(
    name="alter_table_with_foreign_key_delete_cascade",
    depends=["create_table_with_foreign_key_delete_cascade"],
)
def test_alter_table_with_foreign_key_delete_cascade(
    capsys, instance_id, sample_database
):
    samples.alter_table_with_foreign_key_delete_cascade(
        instance_id, sample_database.database_id
    )
    out, _ = capsys.readouterr()
    assert "Altered ShoppingCarts table with FKShoppingCartsCustomerName" in out


@pytest.mark.dependency(depends=["alter_table_with_foreign_key_delete_cascade"])
def test_drop_foreign_key_contraint_delete_cascade(
    capsys, instance_id, sample_database
):
    samples.drop_foreign_key_constraint_delete_cascade(
        instance_id, sample_database.database_id
    )
    out, _ = capsys.readouterr()
    assert "Altered ShoppingCarts table to drop FKShoppingCartsCustomerName" in out


def test_create_sequence(capsys, instance_id, bit_reverse_sequence_database):
    samples.create_sequence(instance_id, bit_reverse_sequence_database.database_id)
    out, _ = capsys.readouterr()
    assert (
        "Created Seq sequence and Customers table, where the key column CustomerId uses the sequence as a default value on database"
        in out
    )
    assert "Number of customer records inserted is 3" in out
    assert "Inserted customer record with Customer Id:" in out


@pytest.mark.dependency(depends=["create_sequence"])
def test_alter_sequence(capsys, instance_id, bit_reverse_sequence_database):
    samples.alter_sequence(instance_id, bit_reverse_sequence_database.database_id)
    out, _ = capsys.readouterr()
    assert (
        "Altered Seq sequence to skip an inclusive range between 1000 and 5000000 on database"
        in out
    )
    assert "Number of customer records inserted is 3" in out
    assert "Inserted customer record with Customer Id:" in out


@pytest.mark.dependency(depends=["alter_sequence"])
def test_drop_sequence(capsys, instance_id, bit_reverse_sequence_database):
    samples.drop_sequence(instance_id, bit_reverse_sequence_database.database_id)
    out, _ = capsys.readouterr()
    assert (
        "Altered Customers table to drop DEFAULT from CustomerId column and dropped the Seq sequence on database"
        in out
    )
