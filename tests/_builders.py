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
from logging import Logger
from mock import create_autospec
from typing import Mapping

from google.cloud.spanner_dbapi import Connection
from google.cloud.spanner_v1 import SpannerClient
from google.cloud.spanner_v1.client import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.session import Session

# Default values used to populate required or expected attributes.
# Tests should not depend on them: if a test requires a specific
# identifier or name, it should set it explicitly.
_PROJECT_ID = "default-project-id"
_INSTANCE_ID = "default-instance-id"
_DATABASE_ID = "default-database-id"


def build_client(**kwargs: Mapping) -> Client:
    """Builds and returns a client for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""

    if "project" not in kwargs:
        kwargs["project"] = _PROJECT_ID

    return Client(**kwargs)


def build_connection(**kwargs: Mapping) -> Connection:
    """Builds and returns a connection for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""

    if "instance" not in kwargs:
        kwargs["instance"] = build_instance()

    if "database" not in kwargs:
        kwargs["database"] = build_database(instance=kwargs["instance"])

    return Connection(**kwargs)


def build_database(**kwargs: Mapping) -> Database:
    """Builds and returns a database for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""

    if "database_id" not in kwargs:
        kwargs["database_id"] = _DATABASE_ID

    if "logger" not in kwargs:
        kwargs["logger"] = build_logger()

    if "instance" not in kwargs:
        kwargs["instance"] = build_instance()

    database = Database(**kwargs)
    database._spanner_api = build_spanner_api()

    return database


def build_instance(**kwargs: Mapping) -> Instance:
    """Builds and returns an instance for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""

    if "instance_id" not in kwargs:
        kwargs["instance_id"] = _INSTANCE_ID

    if "client" not in kwargs:
        kwargs["client"] = build_client()

    return Instance(**kwargs)


def build_logger() -> Logger:
    """Builds and returns a logger for testing."""
    return create_autospec(Logger, instance=True)


def build_session(**kwargs: Mapping) -> Session:
    """Builds and returns a session for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""

    if "database" not in kwargs:
        kwargs["database"] = build_database()

    return Session(**kwargs)


def build_spanner_api() -> SpannerClient:
    """Builds and returns a mock Spanner Client API for testing using the given arguments.
    Commonly used methods are mocked to return default values."""

    return create_autospec(SpannerClient, instance=True)
