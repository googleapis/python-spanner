# Copyright 2024 Google LLC All rights reserved.
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
import unittest
import mock

from . import _helpers
from google.cloud.spanner_v1 import Client, DirectedReadOptions

HAS_OTEL_INSTALLED = False

try:
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON
    from opentelemetry import trace

    HAS_OTEL_INSTALLED = True
except ImportError:
    pass


@pytest.mark.skipif(not HAS_OTEL_INSTALLED, reason="OpenTelemetry needed.")
@pytest.mark.skipif(not _helpers.USE_EMULATOR, reason="Emulator needed.")
class TestObservability(unittest.TestCase):
    PROJECT = "PROJECT"
    PATH = "projects/%s" % (PROJECT,)
    CONFIGURATION_NAME = "config-name"
    INSTANCE_ID = "instance-id"
    INSTANCE_NAME = "%s/instances/%s" % (PATH, INSTANCE_ID)
    DISPLAY_NAME = "display-name"
    DATABASE_ID = "database"
    NODE_COUNT = 5
    PROCESSING_UNITS = 5000
    LABELS = {"test": "true"}
    TIMEOUT_SECONDS = 80
    LEADER_OPTIONS = ["leader1", "leader2"]
    DIRECTED_READ_OPTIONS = {
        "include_replicas": {
            "replica_selections": [
                {
                    "location": "us-west1",
                    "type_": DirectedReadOptions.ReplicaSelection.Type.READ_ONLY,
                },
            ],
            "auto_failover_disabled": True,
        },
    }

    def test_observability_options_propagated_extended_tracing_off(self):
        self.__test_observability_options(True)

    def test_observability_options_propagated(self):
        self.__test_observability_options(False)

    def __test_observability_options(self, enable_extended_tracing):
        global_tracer_provider = TracerProvider(sampler=ALWAYS_ON)
        trace.set_tracer_provider(global_tracer_provider)
        global_trace_exporter = InMemorySpanExporter()
        global_tracer_provider.add_span_processor(
            SimpleSpanProcessor(global_trace_exporter)
        )

        inject_tracer_provider = TracerProvider(sampler=ALWAYS_ON)
        inject_trace_exporter = InMemorySpanExporter()
        inject_tracer_provider.add_span_processor(
            SimpleSpanProcessor(inject_trace_exporter)
        )
        observability_options = dict(
            tracer_provider=inject_tracer_provider,
            enable_extended_tracing=enable_extended_tracing,
        )
        client = Client(
            project=self.PROJECT,
            observability_options=observability_options,
            credentials=_make_credentials(),
        )

        instance = client.instance(
            self.INSTANCE_ID,
            self.CONFIGURATION_NAME,
            display_name=self.DISPLAY_NAME,
            node_count=self.NODE_COUNT,
            labels=self.LABELS,
        )

        db = instance.database(self.DATABASE_ID)
        self.assertEqual(db.observability_options, observability_options)
        with db.snapshot() as snapshot:
            res = snapshot.execute_sql("SELECT 1")
            for val in res:
                _ = val

        from_global_spans = global_trace_exporter.get_finished_spans()
        from_inject_spans = inject_trace_exporter.get_finished_spans()
        self.assertEqual(
            len(from_global_spans),
            0,
            "Expecting no spans from the global trace exporter",
        )
        self.assertEqual(
            len(from_inject_spans) >= 2,
            True,
            "Expecting at least 2 spans from the injected trace exporter",
        )
        gotNames = [span.name for span in from_inject_spans]
        wantNames = ["CloudSpanner.CreateSession", "CloudSpanner.ReadWriteTransaction"]
        self.assertEqual(
            gotNames,
            wantNames,
            "Span names mismatch",
        )

        # Check for conformance of enable_extended_tracing
        lastSpan = from_inject_spans[len(from_inject_spans) - 1]
        wantAnnotatedSQL = "SELECT 1"
        if not enable_extended_tracing:
            wantAnnotatedSQL = None
        self.assertEqual(
            lastSpan.attributes.get("db.statement", None),
            wantAnnotatedSQL,
            "Mismatch in annotated sql",
        )


def _make_credentials():
    import google.auth.credentials

    class _CredentialsWithScopes(
        google.auth.credentials.Credentials, google.auth.credentials.Scoped
    ):
        pass

    return mock.Mock(spec=_CredentialsWithScopes)
