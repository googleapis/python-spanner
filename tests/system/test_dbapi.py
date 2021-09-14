# Copyright 2021 Google LLC All rights reserved.
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

import hashlib
import pickle
import pkg_resources

import pytest

from google.cloud import spanner_v1
from google.cloud.spanner_dbapi.connection import connect, Connection
from . import _helpers

DATABASE_NAME = "dbapi-txn"

DDL_STATEMENTS = (
    """CREATE TABLE contacts (
        contact_id INT64,
        first_name STRING(1024),
        last_name STRING(1024),
        email STRING(1024)
    )
    PRIMARY KEY (contact_id)""",
)


@pytest.fixture(scope="session")
def raw_database(shared_instance, database_operation_timeout):
    databse_id = _helpers.unique_id("dbapi-txn")
    pool = spanner_v1.BurstyPool(labels={"testcase": "database_api"})
    database = shared_instance.database(
        databse_id, ddl_statements=DDL_STATEMENTS, pool=pool,
    )
    op = database.create()
    op.result(database_operation_timeout)  # raises on failure / timeout.

    yield database

    database.drop()


def clear_table(transaction):
    transaction.execute_update("DELETE FROM contacts WHERE true")


@pytest.fixture(scope="function")
def dbapi_database(raw_database):

    raw_database.run_in_transaction(clear_table)

    yield raw_database

    raw_database.run_in_transaction(clear_table)


def test_commit(shared_instance, dbapi_database):
    """Test committing a transaction with several statements."""
    want_row = (
        1,
        "updated-first-name",
        "last-name",
        "test.email_updated@domen.ru",
    )
    # connect to the test database
    conn = Connection(shared_instance, dbapi_database)
    cursor = conn.cursor()

    # execute several DML statements within one transaction
    cursor.execute(
        """
INSERT INTO contacts (contact_id, first_name, last_name, email)
VALUES (1, 'first-name', 'last-name', 'test.email@domen.ru')
    """
    )
    cursor.execute(
        """
UPDATE contacts
SET first_name = 'updated-first-name'
WHERE first_name = 'first-name'
"""
    )
    cursor.execute(
        """
UPDATE contacts
SET email = 'test.email_updated@domen.ru'
WHERE email = 'test.email@domen.ru'
"""
    )
    conn.commit()

    # read the resulting data from the database
    cursor.execute("SELECT * FROM contacts")
    got_rows = cursor.fetchall()
    conn.commit()

    assert got_rows == [want_row]

    cursor.close()
    conn.close()


def test_rollback(shared_instance, dbapi_database):
    """Test rollbacking a transaction with several statements."""
    want_row = (2, "first-name", "last-name", "test.email@domen.ru")
    # connect to the test database
    conn = Connection(shared_instance, dbapi_database)
    cursor = conn.cursor()

    cursor.execute(
        """
INSERT INTO contacts (contact_id, first_name, last_name, email)
VALUES (2, 'first-name', 'last-name', 'test.email@domen.ru')
    """
    )
    conn.commit()

    # execute several DMLs with one transaction
    cursor.execute(
        """
UPDATE contacts
SET first_name = 'updated-first-name'
WHERE first_name = 'first-name'
"""
    )
    cursor.execute(
        """
UPDATE contacts
SET email = 'test.email_updated@domen.ru'
WHERE email = 'test.email@domen.ru'
"""
    )
    conn.rollback()

    # read the resulting data from the database
    cursor.execute("SELECT * FROM contacts")
    got_rows = cursor.fetchall()
    conn.commit()

    assert got_rows == [want_row]

    cursor.close()
    conn.close()


def test_autocommit_mode_change(shared_instance, dbapi_database):
    """Test auto committing a transaction on `autocommit` mode change."""
    want_row = (
        2,
        "updated-first-name",
        "last-name",
        "test.email@domen.ru",
    )
    # connect to the test database
    conn = Connection(shared_instance, dbapi_database)
    cursor = conn.cursor()

    cursor.execute(
        """
INSERT INTO contacts (contact_id, first_name, last_name, email)
VALUES (2, 'first-name', 'last-name', 'test.email@domen.ru')
    """
    )
    cursor.execute(
        """
UPDATE contacts
SET first_name = 'updated-first-name'
WHERE first_name = 'first-name'
"""
    )
    conn.autocommit = True

    # read the resulting data from the database
    cursor.execute("SELECT * FROM contacts")
    got_rows = cursor.fetchall()

    assert got_rows == [want_row]

    cursor.close()
    conn.close()


def test_rollback_on_connection_closing(shared_instance, dbapi_database):
    """
    When closing a connection all the pending transactions
    must be rollbacked. Testing if it's working this way.
    """
    want_row = (1, "first-name", "last-name", "test.email@domen.ru")
    # connect to the test database
    conn = Connection(shared_instance, dbapi_database)
    cursor = conn.cursor()

    cursor.execute(
        """
INSERT INTO contacts (contact_id, first_name, last_name, email)
VALUES (1, 'first-name', 'last-name', 'test.email@domen.ru')
    """
    )
    conn.commit()

    cursor.execute(
        """
UPDATE contacts
SET first_name = 'updated-first-name'
WHERE first_name = 'first-name'
"""
    )
    conn.close()

    # connect again, as the previous connection is no-op after closing
    conn = Connection(shared_instance, dbapi_database)
    cursor = conn.cursor()

    # read the resulting data from the database
    cursor.execute("SELECT * FROM contacts")
    got_rows = cursor.fetchall()
    conn.commit()

    assert got_rows == [want_row]

    cursor.close()
    conn.close()


def test_results_checksum(shared_instance, dbapi_database):
    """Test that results checksum is calculated properly."""
    conn = Connection(shared_instance, dbapi_database)
    cursor = conn.cursor()

    cursor.execute(
        """
INSERT INTO contacts (contact_id, first_name, last_name, email)
VALUES
(1, 'first-name', 'last-name', 'test.email@domen.ru'),
(2, 'first-name2', 'last-name2', 'test.email2@domen.ru')
    """
    )
    assert len(conn._statements) == 1
    conn.commit()

    cursor.execute("SELECT * FROM contacts")
    got_rows = cursor.fetchall()

    assert len(conn._statements) == 1
    conn.commit()

    checksum = hashlib.sha256()
    checksum.update(pickle.dumps(got_rows[0]))
    checksum.update(pickle.dumps(got_rows[1]))

    assert cursor._checksum.checksum.digest() == checksum.digest()


def test_execute_many(shared_instance, dbapi_database):
    # connect to the test database
    conn = Connection(shared_instance, dbapi_database)
    cursor = conn.cursor()

    row_data = [
        (1, "first-name", "last-name", "test.email@example.com"),
        (2, "first-name2", "last-name2", "test.email2@example.com"),
    ]
    cursor.executemany(
        """
INSERT INTO contacts (contact_id, first_name, last_name, email)
VALUES (%s, %s, %s, %s)
    """,
        row_data,
    )
    conn.commit()

    cursor.executemany(
        """SELECT * FROM contacts WHERE contact_id = %s""", ((1,), (2,)),
    )
    res = cursor.fetchall()
    conn.commit()

    assert len(res) == len(row_data)
    for found, expected in zip(res, row_data):
        assert found[0] == expected[0]

    # checking that execute() and executemany()
    # results are not mixed together
    cursor.execute(
        """
SELECT * FROM contacts WHERE contact_id = 1
""",
    )
    res = cursor.fetchone()
    conn.commit()

    assert res[0] == 1
    conn.close()


def test_DDL_autocommit(shared_instance, dbapi_database):
    """Check that DDLs in autocommit mode are immediately executed."""
    conn = Connection(shared_instance, dbapi_database)
    conn.autocommit = True

    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE Singers (
            SingerId     INT64 NOT NULL,
            Name    STRING(1024),
        ) PRIMARY KEY (SingerId)
    """
    )
    conn.close()

    # if previous DDL wasn't committed, the next DROP TABLE
    # statement will fail with a ProgrammingError
    conn = Connection(shared_instance, dbapi_database)
    cur = conn.cursor()

    cur.execute("DROP TABLE Singers")
    conn.commit()


def test_DDL_commit(shared_instance, dbapi_database):
    """Check that DDLs in commit mode are executed on calling `commit()`."""
    conn = Connection(shared_instance, dbapi_database)
    cur = conn.cursor()

    cur.execute(
        """
    CREATE TABLE Singers (
        SingerId     INT64 NOT NULL,
        Name    STRING(1024),
    ) PRIMARY KEY (SingerId)
    """
    )
    conn.commit()
    conn.close()

    # if previous DDL wasn't committed, the next DROP TABLE
    # statement will fail with a ProgrammingError
    conn = Connection(shared_instance, dbapi_database)
    cur = conn.cursor()

    cur.execute("DROP TABLE Singers")
    conn.commit()


def test_ping(shared_instance, dbapi_database):
    """Check connection validation method."""
    conn = Connection(shared_instance, dbapi_database)
    conn.validate()
    conn.close()


def test_user_agent(shared_instance, dbapi_database):
    """Check that DB API uses an appropriate user agent."""
    conn = connect(shared_instance.name, dbapi_database.name)
    assert (
        conn.instance._client._client_info.user_agent
        == "dbapi/" + pkg_resources.get_distribution("google-cloud-spanner").version
    )
