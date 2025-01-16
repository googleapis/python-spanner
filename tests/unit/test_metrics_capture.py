import pytest
from unittest import mock
from google.cloud.spanner_v1.metrics.metrics_capture import MetricsCapture
from google.cloud.spanner_v1.metrics.metrics_tracer_factory import MetricsTracerFactory


@pytest.fixture
def mock_tracer_factory():
    with mock.patch.object(
        MetricsTracerFactory, "create_metrics_tracer"
    ) as mock_create:
        yield mock_create


def test_metrics_capture_enter(mock_tracer_factory):
    mock_tracer = mock.Mock()
    mock_tracer_factory.return_value = mock_tracer

    with MetricsCapture() as capture:
        assert capture is not None
        mock_tracer_factory.assert_called_once()
        mock_tracer.record_operation_start.assert_called_once()


def test_metrics_capture_exit(mock_tracer_factory):
    mock_tracer = mock.Mock()
    mock_tracer_factory.return_value = mock_tracer

    with MetricsCapture():
        pass

    mock_tracer.record_operation_completion.assert_called_once()
