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

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def mock_periodic_exporting_metric_reader():
    """Globally mock PeriodicExportingMetricReader to prevent real network calls."""
    with patch(
        "google.cloud.spanner_v1.client.PeriodicExportingMetricReader"
    ) as mock_client_reader, patch(
        "opentelemetry.sdk.metrics.export.PeriodicExportingMetricReader"
    ):
        yield mock_client_reader


@pytest.fixture(autouse=True)
def clear_otel_exporter():
    """Clear the OpenTelemetry span exporter before and after each test to prevent leakage."""
    try:
        from tests._helpers import HAS_OPENTELEMETRY_INSTALLED, get_test_ot_exporter

        if HAS_OPENTELEMETRY_INSTALLED:
            exporter = get_test_ot_exporter()
            if exporter:
                exporter.clear()
    except ImportError:
        pass

    yield

    try:
        from tests._helpers import HAS_OPENTELEMETRY_INSTALLED, get_test_ot_exporter

        if HAS_OPENTELEMETRY_INSTALLED:
            exporter = get_test_ot_exporter()
            if exporter:
                exporter.clear()
    except ImportError:
        pass
