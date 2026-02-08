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


@pytest.fixture
def clean_factory():
    """Return a factory with project_id and instance_id cleared for test isolation."""
    factory = SpannerMetricsTracerFactory()
    factory._client_attributes.pop("instance_id", None)
    factory._client_attributes.pop("project_id", None)
    return factory


@pytest.fixture
def mock_tracer(clean_factory):
    """Set up a clean MockMetricTracer and return the factory."""
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    return clean_factory


# --- Parsing tests ---


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


# --- _set_metrics_tracer_attributes tests ---


def test_set_metrics_tracer_attributes(interceptor, mock_tracer):
    """Verify tracer receives project, instance, and database from resources."""
    resources = {
        "project": "my_project",
        "instance": "my_instance",
        "database": "my_database",
    }

    interceptor._set_metrics_tracer_attributes(resources)

    tracer = SpannerMetricsTracerFactory.current_metrics_tracer
    assert tracer.client_attributes.get("project_id") == "my_project"
    assert tracer.client_attributes.get("instance_id") == "my_instance"
    assert tracer.client_attributes.get("database") == "my_database"


def test_set_metrics_tracer_attributes_updates_factory(interceptor, mock_tracer):
    """Verify that the factory's client_attributes are updated with project and instance."""
    resources = {
        "project": "test_project",
        "instance": "test_instance",
        "database": "test_database",
    }

    interceptor._set_metrics_tracer_attributes(resources)

    # Verify factory attributes are updated
    assert mock_tracer.client_attributes.get("instance_id") == "test_instance"
    assert mock_tracer.client_attributes.get("project_id") == "test_project"
    # Database should NOT be set in factory (it may vary per operation)
    assert "database" not in mock_tracer.client_attributes


def test_set_metrics_tracer_attributes_no_tracer(interceptor, clean_factory):
    """Verify that nothing happens when current_metrics_tracer is None."""
    SpannerMetricsTracerFactory.current_metrics_tracer = None

    resources = {
        "project": "test_project",
        "instance": "test_instance",
    }

    interceptor._set_metrics_tracer_attributes(resources)

    # Factory should NOT be updated when current_metrics_tracer is None
    assert "instance_id" not in clean_factory.client_attributes
    assert "project_id" not in clean_factory.client_attributes


def test_set_metrics_tracer_attributes_empty_resources(interceptor, mock_tracer):
    """Verify that nothing happens when resources is empty."""
    interceptor._set_metrics_tracer_attributes({})

    assert "instance_id" not in mock_tracer.client_attributes
    assert "project_id" not in mock_tracer.client_attributes


def test_set_metrics_tracer_attributes_none_resources(interceptor, mock_tracer):
    """Verify that nothing happens when resources is None."""
    interceptor._set_metrics_tracer_attributes(None)

    assert "instance_id" not in mock_tracer.client_attributes
    assert "project_id" not in mock_tracer.client_attributes


def test_set_metrics_tracer_attributes_partial_resources_project_only(
    interceptor, mock_tracer
):
    """Verify that only project is set when instance is missing."""
    resources = {"project": "test_project"}

    interceptor._set_metrics_tracer_attributes(resources)

    # Only project should be set
    assert mock_tracer.client_attributes.get("project_id") == "test_project"
    assert "instance_id" not in mock_tracer.client_attributes
    tracer = SpannerMetricsTracerFactory.current_metrics_tracer
    assert tracer.client_attributes.get("project_id") == "test_project"
    assert "instance_id" not in tracer.client_attributes


def test_set_metrics_tracer_attributes_partial_resources_instance_only(
    interceptor, mock_tracer
):
    """Verify that only instance is set when project is missing."""
    resources = {"instance": "test_instance"}

    interceptor._set_metrics_tracer_attributes(resources)

    # Only instance should be set
    assert mock_tracer.client_attributes.get("instance_id") == "test_instance"
    assert "project_id" not in mock_tracer.client_attributes
    tracer = SpannerMetricsTracerFactory.current_metrics_tracer
    assert tracer.client_attributes.get("instance_id") == "test_instance"
    assert "project_id" not in tracer.client_attributes


def test_set_metrics_tracer_attributes_partial_resources_database_only(
    interceptor, mock_tracer
):
    """Verify that database is set on tracer but NOT propagated to factory."""
    resources = {"database": "test_database"}

    interceptor._set_metrics_tracer_attributes(resources)

    tracer = SpannerMetricsTracerFactory.current_metrics_tracer
    assert tracer.client_attributes.get("database") == "test_database"
    # Factory should NOT have database, project, or instance
    assert "database" not in mock_tracer.client_attributes
    assert "project_id" not in mock_tracer.client_attributes
    assert "instance_id" not in mock_tracer.client_attributes


def test_set_metrics_tracer_attributes_overwrites_stale_tracer_values(interceptor):
    """Verify that request resource values replace stale tracer client_attributes."""
    stale_tracer = MockMetricTracer()
    # Directly populate to simulate stale values from factory copy
    stale_tracer.client_attributes["project_id"] = "stale_project"
    stale_tracer.client_attributes["instance_id"] = "stale_instance"
    stale_tracer.client_attributes["database"] = "stale_database"

    SpannerMetricsTracerFactory.current_metrics_tracer = stale_tracer
    resources = {
        "project": "fresh_project",
        "instance": "fresh_instance",
        "database": "fresh_database",
    }

    interceptor._set_metrics_tracer_attributes(resources)

    assert stale_tracer.client_attributes.get("project_id") == "fresh_project"
    assert stale_tracer.client_attributes.get("instance_id") == "fresh_instance"
    assert stale_tracer.client_attributes.get("database") == "fresh_database"


def test_new_tracer_inherits_factory_attributes(interceptor, mock_tracer):
    """
    Integration test: Verify that a new tracer created after
    _set_metrics_tracer_attributes inherits project and instance from factory.

    This is the core test for the bug fix - ensuring that subsequent operations
    get tracers with the correct attributes.
    """
    resources = {
        "project": "inherited_project",
        "instance": "inherited_instance",
        "database": "db1",
    }
    interceptor._set_metrics_tracer_attributes(resources)

    # Simulate second operation: create a new tracer from factory
    new_tracer = mock_tracer.create_metrics_tracer()

    if new_tracer is None:
        pytest.skip("OpenTelemetry not installed; cannot verify tracer inheritance")

    assert new_tracer.client_attributes.get("project_id") == "inherited_project"
    assert new_tracer.client_attributes.get("instance_id") == "inherited_instance"
    # Database should NOT be inherited (it's per-operation)
    assert "database" not in new_tracer.client_attributes


# --- intercept tests ---


def test_intercept_with_tracer(interceptor):
    """Verify intercept records metrics and invokes the gRPC method."""
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


def test_intercept_no_tracer(interceptor):
    """Verify that intercept returns directly when current_metrics_tracer is None."""
    SpannerMetricsTracerFactory.current_metrics_tracer = None

    invoked_response = MagicMock()
    mock_invoked_method = MagicMock(return_value=invoked_response)
    call_details = MagicMock(method="spanner.someMethod", metadata=[])

    response = interceptor.intercept(mock_invoked_method, "request", call_details)
    assert response == invoked_response
    mock_invoked_method.assert_called_once_with("request", call_details)


def test_intercept_factory_disabled(interceptor):
    """Verify that intercept returns directly when factory is disabled."""
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    factory = SpannerMetricsTracerFactory()
    original_enabled = factory.enabled

    try:
        factory.enabled = False

        invoked_response = MagicMock()
        mock_invoked_method = MagicMock(return_value=invoked_response)
        call_details = MagicMock(method="spanner.someMethod", metadata=[])

        response = interceptor.intercept(mock_invoked_method, "request", call_details)
        assert response == invoked_response
        mock_invoked_method.assert_called_once_with("request", call_details)
    finally:
        factory.enabled = original_enabled


# --- MockMetricTracer ---


class MockMetricTracer:
    """Mock that mirrors MetricsTracer's set-once semantics.

    The real MetricsTracer.set_project/set_instance/set_database only write
    if the key is absent from _client_attributes. This guard is critical for
    testing the pop-then-set pattern in _set_metrics_tracer_attributes.
    """

    def __init__(self):
        self.method = None
        self.client_attributes = {}
        self.gfe_enabled = False

    def set_project(self, project):
        if "project_id" not in self.client_attributes:
            self.client_attributes["project_id"] = project

    def set_instance(self, instance):
        if "instance_id" not in self.client_attributes:
            self.client_attributes["instance_id"] = instance

    def set_database(self, database):
        if "database" not in self.client_attributes:
            self.client_attributes["database"] = database

    def set_method(self, method):
        self.method = method

    def record_attempt_start(self):
        pass

    def record_attempt_completion(self):
        pass
