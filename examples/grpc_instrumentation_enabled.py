# -*- coding: utf-8 -*-
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

import os
import time

import google.cloud.spanner as spanner
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry import trace

# Enable the gRPC instrumentation if you'd like more introspection.
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient
grpc_client_instrumentor = GrpcInstrumentorClient()
grpc_client_instrumentor.instrument()

def main():
    # Setup common variables that'll be used between Spanner and traces.
    project_id = os.environ.get('SPANNER_PROJECT_ID')

    # Setup OpenTelemetry, trace and Cloud Trace exporter.
    sampler = ALWAYS_ON
    tracerProvider = TracerProvider(sampler=sampler)
    traceExporter = CloudTraceSpanExporter(project_id)
    tracerProvider.add_span_processor(
        BatchSpanProcessor(traceExporter))
    trace.set_tracer_provider(tracerProvider)
    # Retrieve the set shared tracer.
    tracer = tracerProvider.get_tracer('cloud.google.com/python/spanner', spanner.__version__)

    # Setup the Cloud Spanner Client.
    # Here we directly pass in the tracerProvider into the spanner client.
    opts = dict(tracer_provider=tracerProvider)
    spanner_client = spanner.Client(project_id, observability_options=opts)

    instance = spanner_client.instance('test-instance')
    database = instance.database('test-db')

    # Now run our queries
    with tracer.start_as_current_span('QueryInformationSchema'):
        with database.snapshot() as snapshot:
            with tracer.start_as_current_span('InformationSchema'):
                info_schema = snapshot.execute_sql(
                    'SELECT * FROM INFORMATION_SCHEMA.TABLES')
                for row in info_schema:
                    print(row)

        with tracer.start_as_current_span('ServerTimeQuery'):
            with database.snapshot() as snapshot:
                # Purposefully issue a bad SQL statement to examine exceptions
                # that get recorded and a ERROR span status.
                data = snapshot.execute_sql('SELECT CURRENT_TIMESTAMP()')
                for row in data:
                    print(row)

if __name__ == '__main__':
    main()
