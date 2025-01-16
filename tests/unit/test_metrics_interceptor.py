import pytest
from google.cloud.spanner_v1.metrics.metrics_interceptor import MetricsInterceptor
from google.cloud.spanner_v1.metrics.spanner_metrics_tracer_factory import (
    SpannerMetricsTracerFactory,
)
from unittest.mock import MagicMock


@pytest.fixture
def interceptor():
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


def test_intercept_without_tracer(interceptor):
    mock_invoked_method = MagicMock(return_value="response")
    mock_details = MagicMock(metadata={})
    response = interceptor.intercept(mock_invoked_method, "request", mock_details)

    assert response == "response"
    mock_invoked_method.assert_called_once_with("request", mock_details)


def test_intercept_with_tracer(interceptor):
    SpannerMetricsTracerFactory.current_metrics_tracer = MockMetricTracer()
    SpannerMetricsTracerFactory.current_metrics_tracer.record_attempt_start = (
        MagicMock()
    )
    SpannerMetricsTracerFactory.current_metrics_tracer.record_attempt_completion = (
        MagicMock()
    )

    mock_invoked_method = MagicMock(return_value="response")
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

    assert response == "response"
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
