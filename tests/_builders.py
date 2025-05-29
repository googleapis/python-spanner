# Copyright 2025 Google LLC All rights reserved.
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

# Default identifiers and names. These are used to populate required
# attributes, but tests should not depend on them. If a test requires
# a specific identifier or name, it should set it explicitly.
_PROJECT_ID = "default-project-id"
_INSTANCE_ID = "default-instance-id"
_DATABASE_ID = "default-database-id"
_SESSION_ID = "default-session-id"
_TRANSACTION_ID = b"default-transaction-id"
_PRECOMMIT_TOKEN = b"default-precommit-token"
_PRECOMMIT_SEQ_NUM = -1

_PROJECT_NAME = "projects/" + _PROJECT_ID
_INSTANCE_NAME = _PROJECT_NAME + "/instances/" + _INSTANCE_ID
_DATABASE_NAME = _INSTANCE_NAME + "/databases/" + _DATABASE_ID
_SESSION_NAME = _DATABASE_NAME + "/sessions/" + _SESSION_ID

# Protocol buffers
# ----------------


def build_precommit_token_pb(**kwargs):
    """Builds and returns a precommit token protocol buffer using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.types import MultiplexedSessionPrecommitToken

    return MultiplexedSessionPrecommitToken(**kwargs)


def build_session_pb(**kwargs):
    """Builds and returns a session protocol buffer using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.types import Session

    if "name" not in kwargs:
        kwargs["name"] = _SESSION_NAME

    return Session(**kwargs)


def build_result_set_pb(**kwargs):
    """Builds and returns a result set protocol buffer using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.types import ResultSet

    if "metadata" not in kwargs or isinstance(kwargs["metadata"], dict):
        metadata_args = kwargs.pop("metadata", {})
        kwargs["metadata"] = build_result_set_metadata_pb(**metadata_args)

    if "precommit_token" not in kwargs or isinstance(kwargs["precommit_token"], dict):
        precommit_token_args = kwargs.pop("precommit_token", {})
        kwargs["precommit_token"] = build_precommit_token_pb(**precommit_token_args)

    return ResultSet(**kwargs)


def build_result_set_metadata_pb(**kwargs):
    """Builds and returns a result set metadata protocol buffer using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.types import ResultSetMetadata

    if "transaction" not in kwargs or isinstance(kwargs["transaction"], dict):
        transaction_args = kwargs.pop("transaction", {})
        kwargs["transaction"] = build_transaction_pb(**transaction_args)

    return ResultSetMetadata(**kwargs)


def build_transaction_pb(**kwargs):
    """Builds and returns a transaction protocol buffer using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.types import Transaction

    if "id" not in kwargs:
        kwargs["id"] = _TRANSACTION_ID

    return Transaction(**kwargs)


# Client objects
# --------------


def build_database(**kwargs):
    """Builds and returns a database for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1 import SpannerClient
    from google.cloud.spanner_v1.database import Database
    from mock.mock import create_autospec

    if "database_id" not in kwargs:
        kwargs["database_id"] = _DATABASE_ID

    if "instance" not in kwargs or isinstance(kwargs["instance"], dict):
        instance_args = kwargs.pop("instance", {})
        kwargs["instance"] = _build_instance(**instance_args)

    database = Database(**kwargs)

    # Mock API calls. Callers can override this to test specific behaviours.
    api = database._spanner_api = create_autospec(SpannerClient, instance=True)
    api.create_session.return_value = build_session_pb()
    api.begin_transaction.return_value = build_transaction_pb()

    return database


def build_session(**kwargs):
    """Builds and returns a session for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.session import Session

    if "database" not in kwargs or isinstance(kwargs["database"], dict):
        database_args = kwargs.pop("database", {})
        kwargs["database"] = build_database(**database_args)

    return Session(**kwargs)


def build_transaction(**kwargs):
    """Builds and returns a transaction for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""

    session = (
        build_session(**kwargs.pop("session", {}))
        if "session" not in kwargs
        else kwargs["session"]
    )

    # Session must be created before building transaction.
    if session.session_id is None:
        session.create()

    return session.transaction()


def _build_client(**kwargs):
    """Builds and returns a client for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1 import Client

    if "project" not in kwargs:
        kwargs["project"] = _PROJECT_ID

    return Client(**kwargs)


def _build_instance(**kwargs):
    """Builds and returns an instance for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.instance import Instance

    if "instance_id" not in kwargs:
        kwargs["instance_id"] = _INSTANCE_ID

    if "client" not in kwargs or isinstance(kwargs["client"], dict):
        client_args = kwargs.pop("client", {})
        kwargs["client"] = _build_client(**client_args)

    return Instance(**kwargs)
