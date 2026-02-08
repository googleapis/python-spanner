# Copyright 2025 Google LLC
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

import pytest
from google.cloud.spanner_v1.metrics.metrics_interceptor import MetricsInterceptor
from google.cloud.spanner_v1.metrics.spanner_metrics_tracer_factory import (
    SpannerMetricsTracerFactory,
)
from unittest.mock import MagicMock


@pytest.fixture
def interceptor():
    SpannerMetricsTracerFactory(enabled=True)
    return MetricsInterceptor()


def test_parse_resource_path_valid(interceptor):
    path = "projects/my_project/instances/my_instance/databases/my_database"
    expected = {
        "project": "my_project",
        "instance": "my_instance",
        "database": "my_database",
    }
    assert interceptor._parse_resource_path(path) == expected


def test_parse_resource_path_invalid(interceptor):
    path = "invalid/path"
    expected = {}
    assert interceptor._parse_resource_path(path) == expected


def test_extract_resource_from_path(interceptor):
    metadata = [
        (
            "google-cloud-resource-prefix",
            "projects/my_project/instances/my_instance/databases/my_database",
        )
    ]
    expected = {
        "project": "my_project",
        "instance": "my_instance",
        "database": "my_database",
    }
    assert interceptor._extract_resource_from_path(metadata) == expected


def test_set_metrics_tracer_attributes(interceptor):
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    resources = {
        "project": "my_project",
        "instance": "my_instance",
        "database": "my_database",
    }

    interceptor._set_metrics_tracer_attributes(resources)
    assert SpannerMetricsTracerFactory.current_metrics_tracer.project == "my_project"
    assert SpannerMetricsTracerFactory.current_metrics_tracer.instance == "my_instance"
    assert SpannerMetricsTracerFactory.current_metrics_tracer.database == "my_database"


def test_set_metrics_tracer_attributes_updates_factory(interceptor):
    """Verify that factory's _client_attributes are also updated."""
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    factory = SpannerMetricsTracerFactory()

    # Reset factory attributes for test isolation
    factory._client_attributes.pop("instance_id", None)
    factory._client_attributes.pop("project_id", None)

    resources = {
        "project": "test_project",
        "instance": "test_instance",
        "database": "test_database",
    }

    interceptor._set_metrics_tracer_attributes(resources)

    # Verify factory attributes are updated
    assert factory.client_attributes.get("instance_id") == "test_instance"
    assert factory.client_attributes.get("project_id") == "test_project"
    # Database should NOT be set in factory (it may vary per operation)
    assert "database_id" not in factory.client_attributes


def test_set_metrics_tracer_attributes_no_tracer(interceptor):
    """Verify that nothing happens when current_metrics_tracer is None."""
    SpannerMetricsTracerFactory.current_metrics_tracer = None
    factory = SpannerMetricsTracerFactory()

    # Reset factory attributes for test isolation
    factory._client_attributes.pop("instance_id", None)
    factory._client_attributes.pop("project_id", None)

    resources = {
        "project": "test_project",
        "instance": "test_instance",
    }

    interceptor._set_metrics_tracer_attributes(resources)

    # Factory should NOT be updated when current_metrics_tracer is None
    assert "instance_id" not in factory.client_attributes
    assert "project_id" not in factory.client_attributes


def test_set_metrics_tracer_attributes_empty_resources(interceptor):
    """Verify that nothing happens when resources is empty."""
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    factory = SpannerMetricsTracerFactory()

    # Reset factory attributes for test isolation
    factory._client_attributes.pop("instance_id", None)
    factory._client_attributes.pop("project_id", None)

    interceptor._set_metrics_tracer_attributes({})

    # Factory should NOT be updated when resources is empty
    assert "instance_id" not in factory.client_attributes
    assert "project_id" not in factory.client_attributes


def test_set_metrics_tracer_attributes_partial_resources_project_only(interceptor):
    """Verify that only project is set when instance is missing."""
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    factory = SpannerMetricsTracerFactory()

    # Reset factory attributes for test isolation
    factory._client_attributes.pop("instance_id", None)
    factory._client_attributes.pop("project_id", None)

    resources = {"project": "test_project"}

    interceptor._set_metrics_tracer_attributes(resources)

    # Only project should be set
    assert factory.client_attributes.get("project_id") == "test_project"
    assert "instance_id" not in factory.client_attributes
    assert SpannerMetricsTracerFactory.current_metrics_tracer.project == "test_project"
    assert SpannerMetricsTracerFactory.current_metrics_tracer.instance is None


def test_set_metrics_tracer_attributes_partial_resources_instance_only(interceptor):
    """Verify that only instance is set when project is missing."""
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    factory = SpannerMetricsTracerFactory()

    # Reset factory attributes for test isolation
    factory._client_attributes.pop("instance_id", None)
    factory._client_attributes.pop("project_id", None)

    resources = {"instance": "test_instance"}

    interceptor._set_metrics_tracer_attributes(resources)

    # Only instance should be set
    assert factory.client_attributes.get("instance_id") == "test_instance"
    assert "project_id" not in factory.client_attributes
    assert (
        SpannerMetricsTracerFactory.current_metrics_tracer.instance == "test_instance"
    )
    assert SpannerMetricsTracerFactory.current_metrics_tracer.project is None


def test_new_tracer_inherits_factory_attributes(interceptor):
    """
    Integration test: Verify that a new tracer created after
    _set_metrics_tracer_attributes inherits project and instance from factory.

    This is the core test for the bug fix - ensuring that subsequent operations
    get tracers with the correct attributes.
    """
    factory = SpannerMetricsTracerFactory()

    # Reset factory attributes for test isolation
    factory._client_attributes.pop("instance_id", None)
    factory._client_attributes.pop("project_id", None)

    # Simulate first operation: set up current tracer and call interceptor
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    resources = {
        "project": "inherited_project",
        "instance": "inherited_instance",
        "database": "db1",
    }
    interceptor._set_metrics_tracer_attributes(resources)

    # Simulate second operation: create a new tracer from factory
    new_tracer = factory.create_metrics_tracer()

    # The new tracer should have project and instance from factory
    # (This is the bug fix: before, these would be missing)
    if new_tracer is not None:  # None if OpenTelemetry not installed
        assert new_tracer.client_attributes.get("project_id") == "inherited_project"
        assert new_tracer.client_attributes.get("instance_id") == "inherited_instance"
        # Database should NOT be inherited (it's per-operation)
        assert "database" not in new_tracer.client_attributes


def test_intercept_with_tracer(interceptor):
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    SpannerMetricsTracerFactory.current_metrics_tracer.record_attempt_start = (
        MagicMock()
    )
    SpannerMetricsTracerFactory.current_metrics_tracer.record_attempt_completion = (
        MagicMock()
    )
    SpannerMetricsTracerFactory.current_metrics_tracer.gfe_enabled = False

    invoked_response = MagicMock()
    invoked_response.initial_metadata.return_value = {}

    mock_invoked_method = MagicMock(return_value=invoked_response)
    call_details = MagicMock(
        method="spanner.someMethod",
        metadata=[
            (
                "google-cloud-resource-prefix",
                "projects/my_project/instances/my_instance/databases/my_database",
            )
        ],
    )

    response = interceptor.intercept(mock_invoked_method, "request", call_details)
    assert response == invoked_response
    SpannerMetricsTracerFactory.current_metrics_tracer.record_attempt_start.assert_called_once()
    SpannerMetricsTracerFactory.current_metrics_tracer.record_attempt_completion.assert_called_once()
    mock_invoked_method.assert_called_once_with("request", call_details)


class MockMetricTracer:
    def __init__(self):
        self.project = None
        self.instance = None
        self.database = None
        self.method = None

    def set_project(self, project):
        self.project = project

    def set_instance(self, instance):
        self.instance = instance

    def set_database(self, database):
        self.database = database

    def set_method(self, method):
        self.method = method

    def record_attempt_start(self):
        pass

    def record_attempt_completion(self):
        pass
