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

"""Interceptor for collecting Cloud Spanner metrics."""

import logging

from grpc_interceptor import ClientInterceptor
from .constants import (
    GOOGLE_CLOUD_RESOURCE_KEY,
    METRIC_LABEL_KEY_DATABASE,
    MONITORED_RES_LABEL_KEY_INSTANCE,
    MONITORED_RES_LABEL_KEY_PROJECT,
    SPANNER_METHOD_PREFIX,
)

from typing import Dict
from .spanner_metrics_tracer_factory import SpannerMetricsTracerFactory
import re

_logger = logging.getLogger(__name__)


class MetricsInterceptor(ClientInterceptor):
    """Interceptor that collects metrics for Cloud Spanner operations."""

    @staticmethod
    def _parse_resource_path(path: str) -> dict:
        """Parse the resource path to extract project, instance and database.

        Args:
            path (str): The resource path from the request

        Returns:
            dict: Extracted resource components
        """
        # Match paths like:
        # projects/{project}/instances/{instance}/databases/{database}/sessions/{session}
        # projects/{project}/instances/{instance}/databases/{database}
        # projects/{project}/instances/{instance}
        pattern = r"^projects/(?P<project>[^/]+)(/instances/(?P<instance>[^/]+))?(/databases/(?P<database>[^/]+))?(/sessions/(?P<session>[^/]+))?.*$"
        match = re.match(pattern, path)
        if match:
            return {k: v for k, v in match.groupdict().items() if v is not None}
        return {}

    @staticmethod
    def _extract_resource_from_path(metadata: Dict[str, str]) -> Dict[str, str]:
        """
        Extracts resource information from the metadata based on the path.

        This method iterates through the metadata dictionary to find the first tuple containing the key 'google-cloud-resource-prefix'. It then extracts the path from this tuple and parses it to extract project, instance, and database information using the _parse_resource_path method.

        Args:
            metadata (Dict[str, str]): A dictionary containing metadata information.

        Returns:
            Dict[str, str]: A dictionary containing extracted project, instance, and database information.
        """
        # Extract resource info from the first metadata tuple containing :path
        path = next(
            (value for key, value in metadata if key == GOOGLE_CLOUD_RESOURCE_KEY), ""
        )

        resources = MetricsInterceptor._parse_resource_path(path)
        return resources

    @staticmethod
    def _remove_prefix(s: str, prefix: str) -> str:
        """
        This function removes the prefix from the given string.

        Args:
            s (str): The string from which the prefix is to be removed.
            prefix (str): The prefix to be removed from the string.

        Returns:
            str: The string with the prefix removed.

        Note:
            This function is used because the `removeprefix` method does not exist in Python 3.8.
        """
        if s.startswith(prefix):
            return s[len(prefix) :]
        return s

    def _set_metrics_tracer_attributes(self, resources: Dict[str, str]) -> None:
        """
        Sets the metric tracer attributes based on the provided resources.

        This method updates the current metric tracer's attributes with the
        project, instance, and database information extracted from the resources
        dictionary. If the current metric tracer is not set, the method does
        nothing.

        Before setting each attribute, any existing value for that key is removed
        from the current tracer's client_attributes to ensure the set_* methods
        (which use set-once semantics) can overwrite stale values from previous
        operations.

        Additionally, this method updates the factory's client attributes for
        project and instance to ensure these values are available for subsequent
        operations. Database is not propagated to the factory because each
        Spanner RPC may target a different database within the same instance.

        Args:
            resources (Dict[str, str]): A dictionary containing project,
                instance, and database information.
        """
        if SpannerMetricsTracerFactory.current_metrics_tracer is None:
            return

        if resources:
            tracer = SpannerMetricsTracerFactory.current_metrics_tracer
            factory = SpannerMetricsTracerFactory()

            # For each resource key, remove the existing value from
            # client_attributes so the tracer's set_* method (which only
            # writes if the key is absent) will accept the fresh value.
            if "project" in resources:
                tracer.client_attributes.pop(MONITORED_RES_LABEL_KEY_PROJECT, None)
                tracer.set_project(resources["project"])
                factory.set_project(resources["project"])

            if "instance" in resources:
                tracer.client_attributes.pop(MONITORED_RES_LABEL_KEY_INSTANCE, None)
                tracer.set_instance(resources["instance"])
                factory.set_instance(resources["instance"])

            if "database" in resources:
                tracer.client_attributes.pop(METRIC_LABEL_KEY_DATABASE, None)
                tracer.set_database(resources["database"])

    def intercept(self, invoked_method, request_or_iterator, call_details):
        """Intercept gRPC calls to collect metrics.

        Args:
            invoked_method: The RPC method
            request_or_iterator: The RPC request
            call_details: Details about the RPC call

        Returns:
            The RPC response
        """
        factory = SpannerMetricsTracerFactory()
        if (
            SpannerMetricsTracerFactory.current_metrics_tracer is None
            or not factory.enabled
        ):
            return invoked_method(request_or_iterator, call_details)

        # Setup Metric Tracer attributes from call details
        try:
            ## Extract Project / Instance / Database from header information
            resources = self._extract_resource_from_path(call_details.metadata)
            self._set_metrics_tracer_attributes(resources)

            ## Format method to be spanner.<method name>
            method_name = self._remove_prefix(
                call_details.method, SPANNER_METHOD_PREFIX
            ).replace("/", ".")

            SpannerMetricsTracerFactory.current_metrics_tracer.set_method(method_name)
            SpannerMetricsTracerFactory.current_metrics_tracer.record_attempt_start()
        except Exception:
            _logger.warning("Failed to set up metrics tracer attributes", exc_info=True)

        response = invoked_method(request_or_iterator, call_details)

        try:
            SpannerMetricsTracerFactory.current_metrics_tracer.record_attempt_completion()

            # Process and send GFE metrics if enabled
            if SpannerMetricsTracerFactory.current_metrics_tracer.gfe_enabled:
                metadata = response.initial_metadata()
                SpannerMetricsTracerFactory.current_metrics_tracer.record_gfe_metrics(
                    metadata
                )
        except Exception:
            _logger.warning("Failed to record metrics", exc_info=True)

        return response
