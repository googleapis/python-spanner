import os
import unittest
import mock

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from opentelemetry.trace.status import StatusCode

    from opentelemetry.semconv.trace import SpanAttributes
    from opentelemetry.semconv.attributes import (
       OTEL_SCOPE_NAME,
       OTEL_SCOPE_VERSION,
    )

    trace.set_tracer_provider(TracerProvider())

    HAS_OPENTELEMETRY_INSTALLED = True
    
    DB_SYSTEM = SpanAttributes.DB_SYSTEM
    DB_NAME = SpanAttributes.DB_NAME
    DB_CONNECTION_STRING = SpanAttributes.DB_CONNECTION_STRING
    NET_HOST_NAME = SpanAttributes.NET_HOST_NAME
    DB_STATEMENT = SpanAttributes.DB_STATEMENT

except ImportError:
    HAS_OPENTELEMETRY_INSTALLED = False

    StatusCode = mock.Mock()
    DB_SYSTEM = "db.system"
    DB_NAME = "db.name"
    DB_CONNECTION_STRING = "db.connection_string"
    NET_HOST_NAME = "net.host.name"
    DB_STATEMENT = "db.statement"
    OTEL_SCOPE_NAME = "otel.scope.name"
    OTEL_SCOPE_VERSION = "otel.scope.version"

_TEST_OT_EXPORTER = None
_TEST_OT_PROVIDER_INITIALIZED = False
EXTENDED_TRACING_ENABLED = os.environ.get('SPANNER_ENABLE_EXTENDED_TRACING', '') == 'true'


def get_test_ot_exporter():
    global _TEST_OT_EXPORTER

    if _TEST_OT_EXPORTER is None:
        _TEST_OT_EXPORTER = InMemorySpanExporter()
    return _TEST_OT_EXPORTER


def use_test_ot_exporter():
    global _TEST_OT_PROVIDER_INITIALIZED

    if _TEST_OT_PROVIDER_INITIALIZED:
        return

    provider = trace.get_tracer_provider()
    if not hasattr(provider, "add_span_processor"):
        return
    provider.add_span_processor(SimpleSpanProcessor(get_test_ot_exporter()))
    _TEST_OT_PROVIDER_INITIALIZED = True


class OpenTelemetryBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if HAS_OPENTELEMETRY_INSTALLED:
            use_test_ot_exporter()
            cls.ot_exporter = get_test_ot_exporter()

    def tearDown(self):
        if HAS_OPENTELEMETRY_INSTALLED:
            self.ot_exporter.clear()

    def assertNoSpans(self):
        if HAS_OPENTELEMETRY_INSTALLED:
            span_list = self.ot_exporter.get_finished_spans()
            self.assertEqual(len(span_list), 0)

    def assertSpanAttributes(
        self, name, status=StatusCode.OK, attributes=None, span=None
    ):
        if HAS_OPENTELEMETRY_INSTALLED:
            if not span:
                span_list = self.ot_exporter.get_finished_spans()
                self.assertEqual(len(span_list), 1)
                span = span_list[0]

            self.assertEqual(span.name, name)
            self.assertEqual(span.status.status_code, status)
            self.assertEqual(dict(span.attributes), attributes)
