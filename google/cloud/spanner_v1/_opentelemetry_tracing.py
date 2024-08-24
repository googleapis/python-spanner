# Copyright 2020 Google LLC All rights reserved.
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

"""Manages OpenTelemetry trace creation and handling"""

from contextlib import contextmanager
import os

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.spanner_v1 import SpannerClient
from google.cloud.spanner_v1 import gapic_version as TRACER_VERSION

try:
    from opentelemetry import trace
    from opentelemetry.trace.status import Status, StatusCode
    from opentelemetry.semconv.trace import SpanAttributes

    HAS_OPENTELEMETRY_INSTALLED = True
    DB_SYSTEM = SpanAttributes.DB_SYSTEM
    DB_NAME = SpanAttributes.DB_NAME
    DB_CONNECTION_STRING = SpanAttributes.DB_CONNECTION_STRING
    NET_HOST_NAME = SpanAttributes.NET_HOST_NAME
    DB_STATEMENT = SpanAttributes.DB_STATEMENT
except ImportError:
    HAS_OPENTELEMETRY_INSTALLED = False


EXTENDED_TRACING_ENABLED = os.environ.get('SPANNER_ENABLE_EXTENDED_TRACING', '') == 'true'
TRACER_NAME = 'cloud.google.com/python/spanner'

def get_tracer(tracer_provider=None):
    """
    get_tracer is a utility to unify and simplify retrieval of the tracer, without
    leaking implementation details given that retrieving a tracer requires providing
    the full qualified library name and version.
    When the tracer_provider is set, it'll retrieve the tracer from it, otherwise
    it'll fall back to the global tracer provider and use this library's specific semantics.
    """
    if tracer_provider:
        return tracerProvider.get_tracer(TRACER_NAME, TRACER_VERSION)
    else:
        return trace.get_tracer(TRACER_NAME, TRACER_VERSION)

@contextmanager
def trace_call(name, session, extra_attributes=None, observability_options=None):
    if not HAS_OPENTELEMETRY_INSTALLED or not session:
        # Empty context manager. Users will have to check if the generated value is None or a span
        yield None
        return

    tracer = None
    if observability_options:
        tracerProvider = observability_options.get('tracer_provider', None)
        if tracerProvider:
            tracer = get_tracer(tracerProvider)

    if tracer is None: # Acquire the global tracer if none was provided.
        tracer = get_tracer()

    # It is imperative that we properly record the Cloud Spanner
    # endpoint tracks whether we are connecting to the emulator
    # or to production.
    spanner_endpoint = os.getenv("SPANNER_EMULATOR_HOST")
    if not spanner_endpoint:
        spanner_endpoint = SpannerClient.DEFAULT_ENDPOINT

    # Set base attributes that we know for every trace created
    attributes = {
        DB_SYSTEM: "spanner",
        DB_CONNECTION_STRING: spanner_endpoint,
        DB_NAME: session._database.name,
        NET_HOST_NAME: spanner_endpoint,
    }

    if extra_attributes:
        attributes.update(extra_attributes)

    extended_tracing = EXTENDED_TRACING_ENABLED or (
                        observability_options and
                        observability_options.get('enable_extended_tracing', False))

    if not extended_tracing:
        attributes.pop(DB_STATEMENT, None)

    with tracer.start_as_current_span(
        name, kind=trace.SpanKind.CLIENT, attributes=attributes
    ) as span:
        try:
            span.set_status(Status(StatusCode.OK))
            yield span
        except GoogleAPICallError as error:
            span.set_status(Status(StatusCode.ERROR))
            span.record_exception(error)
            raise
