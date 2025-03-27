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

# Default identifiers.
PROJECT_ID = "project-id"
INSTANCE_ID = "instance-id"
DATABASE_ID = "database-id"
SESSION_ID = "session-id"
TRANSACTION_ID = b"transaction-id"

# Default names.
INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID
DATABASE_NAME = INSTANCE_NAME + "/databases/" + DATABASE_ID
SESSION_NAME = DATABASE_NAME + "/sessions/" + SESSION_ID


def build_database(**database_kwargs):
    """Builds and returns a database for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1 import Session as SessionProto
    from google.cloud.spanner_v1 import SpannerClient
    from google.cloud.spanner_v1 import Transaction as TransactionProto
    from google.cloud.spanner_v1.database import Database
    from mock.mock import create_autospec

    instance = _build_instance()
    database = Database(database_id=DATABASE_ID, instance=instance)

    # Mock API calls. Callers can override this to test specific behaviours.
    api = database._spanner_api = create_autospec(SpannerClient, instance=True)
    api.create_session.return_value = SessionProto(name=SESSION_NAME)
    api.begin_transaction.return_value = TransactionProto(id=TRANSACTION_ID)

    return database


def build_session(**session_kwargs):
    """Builds and returns a session for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.session import Session

    if not session_kwargs.get("database"):
        session_kwargs["database"] = build_database()

    return Session(**session_kwargs)


def _build_client():
    """Builds and returns a client for testing."""
    from google.cloud.spanner_v1 import Client

    return Client(project=PROJECT_ID)


def _build_instance(**instance_kwargs):
    """Builds and returns an instance for testing."""
    from google.cloud.spanner_v1.instance import Instance

    client = _build_client()
    return Instance(instance_id=INSTANCE_ID, client=client)
