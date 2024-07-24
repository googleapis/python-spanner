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


def annotate_with_sql_statement(span, sql):
    """
    annotate_sql_statement will set the attribute DB_STATEMENT
    to the sql statement, only if SPANNER_ENABLE_EXTENDED_TRACING=true
    is set in the environment.
    """
    if EXTENDED_TRACING_ENABLED:
        span.set_attribute(DB_STATEMENT, sql)


@contextmanager
def trace_call(name, session, extra_attributes=None):
    if not HAS_OPENTELEMETRY_INSTALLED or not session:
        # Empty context manager. Users will have to check if the generated value is None or a span
        yield None
        return

    tracer = trace.get_tracer(__name__)

    # Set base attributes that we know for every trace created
    attributes = {
        DB_SYSTEM: "google.cloud.spanner",
        DB_CONNECTION_STRING: SpannerClient.DEFAULT_ENDPOINT,
        DB_NAME: session._database.name,
        NET_HOST_NAME: SpannerClient.DEFAULT_ENDPOINT,
    }

    if extra_attributes:
        attributes.update(extra_attributes)

    if not EXTENDED_TRACING_ENABLED:
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
