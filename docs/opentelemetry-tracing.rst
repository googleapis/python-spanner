Tracing with OpenTelemetry
==========================

This library uses `OpenTelemetry <https://opentelemetry.io/>`_ to automatically generate traces providing insight on calls to Cloud Spanner. 
For information on the benefits and utility of tracing, see the `Cloud Trace docs <https://cloud.google.com/trace/docs/overview>`_.

To take advantage of these traces, we first need to install OpenTelemetry:

.. code-block:: sh

    pip install opentelemetry-api opentelemetry-sdk opentelemetry-instrumentation
    pip install opentelemetry-exporter-gcp-trace

We also need to tell OpenTelemetry which exporter to use. To export Spanner traces to `Cloud Tracing <https://cloud.google.com/trace>`_, add the following lines to your application:

.. code:: python

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
    from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
    # BatchSpanProcessor exports spans to Cloud Trace
    # in a seperate thread to not block on the main thread
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    # Create and export one trace every 1000 requests
    sampler = TraceIdRatioBased(1/1000)
    # Use the default tracer provider
    trace.set_tracer_provider(TracerProvider(sampler=sampler))
    trace.get_tracer_provider().add_span_processor(
        # Initialize the cloud tracing exporter
        BatchSpanProcessor(CloudTraceSpanExporter())
    )


Alternatively you can pass in a tracer provider into the Cloud Spanner initialization

.. code:: python

    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
    from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter

    # Create and export one trace every 1000 requests
    sampler = TraceIdRatioBased(1/1000)
    tracerProvider = TracerProvider(sampler=sampler)
    tracerProvider.add_span_processor(
        # Initialize the cloud tracing exporter
        BatchSpanProcessor(CloudTraceSpanExporter())
    )

    o11y = dict(tracer_provider=tracerProvider)
    # Pass the tracer provider while creating the Spanner client.
    spanner_client = spanner.Client(observability_options=o11y)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

please not that the tracer being used is retrieved by invoking:

.. code:: python

   tracer = tracerProvider.get_tracer('cloud.google.com/python/spanner', SPANNER_LIB_VERSION)

Generated spanner traces should now be available on `Cloud Trace <https://console.cloud.google.com/traces>`_.

Tracing is most effective when many libraries are instrumented to provide insight over the entire lifespan of a request.
For a list of libraries that can be instrumented, see the `OpenTelemetry Integrations` section of the `OpenTelemetry Python docs <https://opentelemetry-python.readthedocs.io/en/stable/>`_

To allow for SQL statements to be annotated in your spans, please set
the environment variable `SPANNER_ENABLE_EXTENDED_TRACING=true` or please set the configuration field `enable_extended_tracing` to `True` when configuring the Cloud Spanner client.
