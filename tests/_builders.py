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

from mock import create_autospec

# Default values used to populate required or expected attributes.
# Tests should not depend on them: if a test requires a specific
# identifier or name, it should set it explicitly.
_PROJECT_ID = "default-project-id"
_INSTANCE_ID = "default-instance-id"
_DATABASE_ID = "default-database-id"


def build_logger():
    """Builds and returns a logger for testing."""
    from logging import Logger

    return create_autospec(Logger, instance=True)


# Client objects
# --------------


def build_client(**kwargs):
    """Builds and returns a client for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1 import Client

    if "project" not in kwargs:
        kwargs["project"] = _PROJECT_ID

    return Client(**kwargs)


def build_database(**kwargs):
    """Builds and returns a database for testing using the given arguments.
    If a required argument is not provided, a default value will be used.."""
    from google.cloud.spanner_v1.database import Database

    if "database_id" not in kwargs:
        kwargs["database_id"] = _DATABASE_ID

    if "logger" not in kwargs:
        kwargs["logger"] = build_logger()

    if "instance" not in kwargs or isinstance(kwargs["instance"], dict):
        instance_args = kwargs.pop("instance", {})
        kwargs["instance"] = build_instance(**instance_args)

    database = Database(**kwargs)
    database._spanner_api = build_spanner_api()

    return database


def build_instance(**kwargs):
    """Builds and returns an instance for testing using the given arguments.
    If a required argument is not provided, a default value will be used."""
    from google.cloud.spanner_v1.instance import Instance

    if "instance_id" not in kwargs:
        kwargs["instance_id"] = _INSTANCE_ID

    if "client" not in kwargs or isinstance(kwargs["client"], dict):
        client_args = kwargs.pop("client", {})
        kwargs["client"] = build_client(**client_args)

    return Instance(**kwargs)


def build_spanner_api():
    """Builds and returns a mock Spanner Client API for testing using the given arguments.
    Commonly used methods are mocked to return default values."""
    from google.cloud.spanner_v1 import SpannerClient

    return create_autospec(SpannerClient, instance=True)
