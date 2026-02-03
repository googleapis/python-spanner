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
import threading
import time
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


def test_intercept_thread_safety(interceptor):
    # Regression test for race condition where current_metrics_tracer changes mid-call

    # Mock tracers
    tracer_a = MagicMock()
    tracer_a.gfe_enabled = False
    tracer_b = MagicMock()
    tracer_b.gfe_enabled = False

    call_details = MagicMock(
        method="spanner.Commit",
        metadata=[],
    )

    def mock_invoked_method(*args, **kwargs):
        # Simulate network delay to allow thread switch
        time.sleep(0.1)
        return MagicMock()

    def thread_a_func():
        # Set Tracer A
        SpannerMetricsTracerFactory.current_metrics_tracer = tracer_a
        # Call intercept
        interceptor.intercept(mock_invoked_method, None, call_details)

    def thread_b_func():
        time.sleep(0.05)  # Wait for A to start
        # Overwrite with Tracer B
        SpannerMetricsTracerFactory.current_metrics_tracer = tracer_b

    t1 = threading.Thread(target=thread_a_func)
    t2 = threading.Thread(target=thread_b_func)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # Verify that Tracer A was used for completion, NOT Tracer B
    # Because Thread A started with Tracer A, it should finish with Tracer A
    tracer_a.record_attempt_completion.assert_called_once()
    tracer_b.record_attempt_completion.assert_not_called()


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
