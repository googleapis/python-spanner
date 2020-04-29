# -*- coding: utf-8 -*-
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Accesses the google.spanner.admin.instance.v1 InstanceAdmin API."""

import functools
import pkg_resources
import warnings

from google.oauth2 import service_account
import google.api_core.client_options
import google.api_core.gapic_v1.client_info
import google.api_core.gapic_v1.config
import google.api_core.gapic_v1.method
import google.api_core.gapic_v1.routing_header
import google.api_core.grpc_helpers
import google.api_core.operation
import google.api_core.operations_v1
import google.api_core.page_iterator
import google.api_core.path_template
import grpc

from google.cloud.spanner_admin_instance_v1.gapic import enums
from google.cloud.spanner_admin_instance_v1.gapic import instance_admin_client_config
from google.cloud.spanner_admin_instance_v1.gapic.transports import (
    instance_admin_grpc_transport,
)
from google.cloud.spanner_admin_instance_v1.proto import spanner_instance_admin_pb2
from google.cloud.spanner_admin_instance_v1.proto import spanner_instance_admin_pb2_grpc
from google.iam.v1 import iam_policy_pb2
from google.iam.v1 import options_pb2
from google.iam.v1 import policy_pb2
from google.longrunning import operations_pb2
from google.protobuf import empty_pb2
from google.protobuf import field_mask_pb2


_GAPIC_LIBRARY_VERSION = pkg_resources.get_distribution("google-cloud-spanner").version


class InstanceAdminClient(object):
    """
    Cloud Spanner Instance Admin API

    The Cloud Spanner Instance Admin API can be used to create, delete,
    modify and list instances. Instances are dedicated Cloud Spanner serving
    and storage resources to be used by Cloud Spanner databases.

    Each instance has a "configuration", which dictates where the
    serving resources for the Cloud Spanner instance are located (e.g.,
    US-central, Europe). Configurations are created by Google based on
    resource availability.

    Cloud Spanner billing is based on the instances that exist and their
    sizes. After an instance exists, there are no additional
    per-database or per-operation charges for use of the instance
    (though there may be additional network bandwidth charges).
    Instances offer isolation: problems with databases in one instance
    will not affect other instances. However, within an instance
    databases can affect each other. For example, if one database in an
    instance receives a lot of requests and consumes most of the
    instance resources, fewer resources are available for other
    databases in that instance, and their performance may suffer.
    """

    SERVICE_ADDRESS = "spanner.googleapis.com:443"
    """The default address of the service."""

    # The name of the interface for this client. This is the key used to
    # find the method configuration in the client_config dictionary.
    _INTERFACE_NAME = "google.spanner.admin.instance.v1.InstanceAdmin"

    @classmethod
    def from_service_account_file(cls, filename, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
        file.

        Args:
            filename (str): The path to the service account private key json
                file.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            InstanceAdminClient: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_file(filename)
        kwargs["credentials"] = credentials
        return cls(*args, **kwargs)

    from_service_account_json = from_service_account_file

    @classmethod
    def instance_path(cls, project, instance):
        """Return a fully-qualified instance string."""
        return google.api_core.path_template.expand(
            "projects/{project}/instances/{instance}",
            project=project,
            instance=instance,
        )

    @classmethod
    def instance_config_path(cls, project, instance_config):
        """Return a fully-qualified instance_config string."""
        return google.api_core.path_template.expand(
            "projects/{project}/instanceConfigs/{instance_config}",
            project=project,
            instance_config=instance_config,
        )

    @classmethod
    def project_path(cls, project):
        """Return a fully-qualified project string."""
        return google.api_core.path_template.expand(
            "projects/{project}", project=project
        )

    def __init__(
        self,
        transport=None,
        channel=None,
        credentials=None,
        client_config=None,
        client_info=None,
        client_options=None,
    ):
        """Constructor.

        Args:
            transport (Union[~.InstanceAdminGrpcTransport,
                    Callable[[~.Credentials, type], ~.InstanceAdminGrpcTransport]): A transport
                instance, responsible for actually making the API calls.
                The default transport uses the gRPC protocol.
                This argument may also be a callable which returns a
                transport instance. Callables will be sent the credentials
                as the first argument and the default transport class as
                the second argument.
            channel (grpc.Channel): DEPRECATED. A ``Channel`` instance
                through which to make calls. This argument is mutually exclusive
                with ``credentials``; providing both will raise an exception.
            credentials (google.auth.credentials.Credentials): The
                authorization credentials to attach to requests. These
                credentials identify this application to the service. If none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
                This argument is mutually exclusive with providing a
                transport instance to ``transport``; doing so will raise
                an exception.
            client_config (dict): DEPRECATED. A dictionary of call options for
                each method. If not specified, the default configuration is used.
            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you're developing
                your own client library.
            client_options (Union[dict, google.api_core.client_options.ClientOptions]):
                Client options used to set user options on the client. API Endpoint
                should be set through client_options.
        """
        # Raise deprecation warnings for things we want to go away.
        if client_config is not None:
            warnings.warn(
                "The `client_config` argument is deprecated.",
                PendingDeprecationWarning,
                stacklevel=2,
            )
        else:
            client_config = instance_admin_client_config.config

        if channel:
            warnings.warn(
                "The `channel` argument is deprecated; use " "`transport` instead.",
                PendingDeprecationWarning,
                stacklevel=2,
            )

        api_endpoint = self.SERVICE_ADDRESS
        if client_options:
            if type(client_options) == dict:
                client_options = google.api_core.client_options.from_dict(
                    client_options
                )
            if client_options.api_endpoint:
                api_endpoint = client_options.api_endpoint

        # Instantiate the transport.
        # The transport is responsible for handling serialization and
        # deserialization and actually sending data to the service.
        if transport:
            if callable(transport):
                self.transport = transport(
                    credentials=credentials,
                    default_class=instance_admin_grpc_transport.InstanceAdminGrpcTransport,
                    address=api_endpoint,
                )
            else:
                if credentials:
                    raise ValueError(
                        "Received both a transport instance and "
                        "credentials; these are mutually exclusive."
                    )
                self.transport = transport
        else:
            self.transport = instance_admin_grpc_transport.InstanceAdminGrpcTransport(
                address=api_endpoint, channel=channel, credentials=credentials
            )

        if client_info is None:
            client_info = google.api_core.gapic_v1.client_info.ClientInfo(
                gapic_version=_GAPIC_LIBRARY_VERSION
            )
        else:
            client_info.gapic_version = _GAPIC_LIBRARY_VERSION
        self._client_info = client_info

        # Parse out the default settings for retry and timeout for each RPC
        # from the client configuration.
        # (Ordinarily, these are the defaults specified in the `*_config.py`
        # file next to this one.)
        self._method_configs = google.api_core.gapic_v1.config.parse_method_configs(
            client_config["interfaces"][self._INTERFACE_NAME]
        )

        # Save a dictionary of cached API call functions.
        # These are the actual callables which invoke the proper
        # transport methods, wrapped with `wrap_method` to add retry,
        # timeout, and the like.
        self._inner_api_calls = {}

    # Service calls
    def list_instance_configs(
        self,
        parent,
        page_size=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Lists the supported instance configurations for a given project.

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_instance_configs(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_instance_configs(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): Defines an Identity and Access Management (IAM) policy. It is used
                to specify access control policies for Cloud Platform resources.

                A ``Policy`` is a collection of ``bindings``. A ``binding`` binds one or
                more ``members`` to a single ``role``. Members can be user accounts,
                service accounts, Google groups, and domains (such as G Suite). A
                ``role`` is a named list of permissions (defined by IAM or configured by
                users). A ``binding`` can optionally specify a ``condition``, which is a
                logic expression that further constrains the role binding based on
                attributes about the request and/or target resource.

                **JSON Example**

                ::

                    {
                      "bindings": [
                        {
                          "role": "roles/resourcemanager.organizationAdmin",
                          "members": [
                            "user:mike@example.com",
                            "group:admins@example.com",
                            "domain:google.com",
                            "serviceAccount:my-project-id@appspot.gserviceaccount.com"
                          ]
                        },
                        {
                          "role": "roles/resourcemanager.organizationViewer",
                          "members": ["user:eve@example.com"],
                          "condition": {
                            "title": "expirable access",
                            "description": "Does not grant access after Sep 2020",
                            "expression": "request.time <
                            timestamp('2020-10-01T00:00:00.000Z')",
                          }
                        }
                      ]
                    }

                **YAML Example**

                ::

                    bindings:
                    - members:
                      - user:mike@example.com
                      - group:admins@example.com
                      - domain:google.com
                      - serviceAccount:my-project-id@appspot.gserviceaccount.com
                      role: roles/resourcemanager.organizationAdmin
                    - members:
                      - user:eve@example.com
                      role: roles/resourcemanager.organizationViewer
                      condition:
                        title: expirable access
                        description: Does not grant access after Sep 2020
                        expression: request.time < timestamp('2020-10-01T00:00:00.000Z')

                For a description of IAM and its features, see the `IAM developer's
                guide <https://cloud.google.com/iam/docs>`__.
            page_size (int): The maximum number of resources contained in the
                underlying API response. If page streaming is performed per-
                resource, this parameter does not affect the return value. If page
                streaming is performed per-page, this determines the maximum number
                of resources in a page.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.api_core.page_iterator.PageIterator` instance.
            An iterable of :class:`~google.cloud.spanner_admin_instance_v1.types.InstanceConfig` instances.
            You can also iterate over the pages of the response
            using its `pages` property.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "list_instance_configs" not in self._inner_api_calls:
            self._inner_api_calls[
                "list_instance_configs"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_instance_configs,
                default_retry=self._method_configs["ListInstanceConfigs"].retry,
                default_timeout=self._method_configs["ListInstanceConfigs"].timeout,
                client_info=self._client_info,
            )

        request = spanner_instance_admin_pb2.ListInstanceConfigsRequest(
            parent=parent, page_size=page_size
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("parent", parent)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        iterator = google.api_core.page_iterator.GRPCIterator(
            client=None,
            method=functools.partial(
                self._inner_api_calls["list_instance_configs"],
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            ),
            request=request,
            items_field="instance_configs",
            request_token_field="page_token",
            response_token_field="next_page_token",
        )
        return iterator

    def get_instance_config(
        self,
        name,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Gets information about a particular instance configuration.

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> name = client.instance_config_path('[PROJECT]', '[INSTANCE_CONFIG]')
            >>>
            >>> response = client.get_instance_config(name)

        Args:
            name (str): Associates a list of ``members`` to a ``role``. Optionally may
                specify a ``condition`` that determines when binding is in effect.
                ``bindings`` with no members will result in an error.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_instance_v1.types.InstanceConfig` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "get_instance_config" not in self._inner_api_calls:
            self._inner_api_calls[
                "get_instance_config"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_instance_config,
                default_retry=self._method_configs["GetInstanceConfig"].retry,
                default_timeout=self._method_configs["GetInstanceConfig"].timeout,
                client_info=self._client_info,
            )

        request = spanner_instance_admin_pb2.GetInstanceConfigRequest(name=name)
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("name", name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["get_instance_config"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def list_instances(
        self,
        parent,
        page_size=None,
        filter_=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Lists all instances in the given project.

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_instances(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_instances(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): If true, this location is designated as the default leader location
                where leader replicas are placed. See the `region types
                documentation <https://cloud.google.com/spanner/docs/instances#region_types>`__
                for more details.
            page_size (int): The maximum number of resources contained in the
                underlying API response. If page streaming is performed per-
                resource, this parameter does not affect the return value. If page
                streaming is performed per-page, this determines the maximum number
                of resources in a page.
            filter_ (str): Associates ``members`` with a ``role``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.api_core.page_iterator.PageIterator` instance.
            An iterable of :class:`~google.cloud.spanner_admin_instance_v1.types.Instance` instances.
            You can also iterate over the pages of the response
            using its `pages` property.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "list_instances" not in self._inner_api_calls:
            self._inner_api_calls[
                "list_instances"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_instances,
                default_retry=self._method_configs["ListInstances"].retry,
                default_timeout=self._method_configs["ListInstances"].timeout,
                client_info=self._client_info,
            )

        request = spanner_instance_admin_pb2.ListInstancesRequest(
            parent=parent, page_size=page_size, filter=filter_
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("parent", parent)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        iterator = google.api_core.page_iterator.GRPCIterator(
            client=None,
            method=functools.partial(
                self._inner_api_calls["list_instances"],
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            ),
            request=request,
            items_field="instances",
            request_token_field="page_token",
            response_token_field="next_page_token",
        )
        return iterator

    def get_instance(
        self,
        name,
        field_mask=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Gets information about a particular instance.

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> name = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> response = client.get_instance(name)

        Args:
            name (str): The normal response of the operation in case of success. If the
                original method returns no data on success, such as ``Delete``, the
                response is ``google.protobuf.Empty``. If the original method is
                standard ``Get``/``Create``/``Update``, the response should be the
                resource. For other methods, the response should have the type
                ``XxxResponse``, where ``Xxx`` is the original method name. For example,
                if the original method name is ``TakeSnapshot()``, the inferred response
                type is ``TakeSnapshotResponse``.
            field_mask (Union[dict, ~google.cloud.spanner_admin_instance_v1.types.FieldMask]): Required. The number of nodes allocated to this instance. This may
                be zero in API responses for instances that are not yet in state
                ``READY``.

                See `the
                documentation <https://cloud.google.com/spanner/docs/instances#node_count>`__
                for more information about nodes.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_instance_v1.types.FieldMask`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_instance_v1.types.Instance` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "get_instance" not in self._inner_api_calls:
            self._inner_api_calls[
                "get_instance"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_instance,
                default_retry=self._method_configs["GetInstance"].retry,
                default_timeout=self._method_configs["GetInstance"].timeout,
                client_info=self._client_info,
            )

        request = spanner_instance_admin_pb2.GetInstanceRequest(
            name=name, field_mask=field_mask
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("name", name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["get_instance"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def create_instance(
        self,
        parent,
        instance_id,
        instance,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Signed seconds of the span of time. Must be from -315,576,000,000 to
        +315,576,000,000 inclusive. Note: these bounds are computed from: 60
        sec/min \* 60 min/hr \* 24 hr/day \* 365.25 days/year \* 10000 years

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> parent = client.project_path('[PROJECT]')
            >>>
            >>> # TODO: Initialize `instance_id`:
            >>> instance_id = ''
            >>>
            >>> # TODO: Initialize `instance`:
            >>> instance = {}
            >>>
            >>> response = client.create_instance(parent, instance_id, instance)
            >>>
            >>> def callback(operation_future):
            ...     # Handle result.
            ...     result = operation_future.result()
            >>>
            >>> response.add_done_callback(callback)
            >>>
            >>> # Handle metadata.
            >>> metadata = response.metadata()

        Args:
            parent (str): Returns permissions that a caller has on the specified resource. If
                the resource does not exist, this will return an empty set of
                permissions, not a NOT_FOUND error.

                Note: This operation is designed to be used for building
                permission-aware UIs and command-line tools, not for authorization
                checking. This operation may "fail open" without warning.
            instance_id (str): JSON name of this field. The value is set by protocol compiler. If
                the user has set a "json_name" option on this field, that option's value
                will be used. Otherwise, it's deduced from the field's name by
                converting it to camelCase.
            instance (Union[dict, ~google.cloud.spanner_admin_instance_v1.types.Instance]): The request for ``CreateInstance``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_instance_v1.types.Instance`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.api_core.operation.Operation` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "create_instance" not in self._inner_api_calls:
            self._inner_api_calls[
                "create_instance"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.create_instance,
                default_retry=self._method_configs["CreateInstance"].retry,
                default_timeout=self._method_configs["CreateInstance"].timeout,
                client_info=self._client_info,
            )

        request = spanner_instance_admin_pb2.CreateInstanceRequest(
            parent=parent, instance_id=instance_id, instance=instance
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("parent", parent)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        operation = self._inner_api_calls["create_instance"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )
        return google.api_core.operation.from_gapic(
            operation,
            self.transport._operations_client,
            spanner_instance_admin_pb2.Instance,
            metadata_type=spanner_instance_admin_pb2.CreateInstanceMetadata,
        )

    def update_instance(
        self,
        instance,
        field_mask,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Input and output type names. These are resolved in the same way as
        FieldDescriptorProto.type_name, but must refer to a message type.

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> # TODO: Initialize `instance`:
            >>> instance = {}
            >>>
            >>> # TODO: Initialize `field_mask`:
            >>> field_mask = {}
            >>>
            >>> response = client.update_instance(instance, field_mask)
            >>>
            >>> def callback(operation_future):
            ...     # Handle result.
            ...     result = operation_future.result()
            >>>
            >>> response.add_done_callback(callback)
            >>>
            >>> # Handle metadata.
            >>> metadata = response.metadata()

        Args:
            instance (Union[dict, ~google.cloud.spanner_admin_instance_v1.types.Instance]): See ``HttpRule``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_instance_v1.types.Instance`
            field_mask (Union[dict, ~google.cloud.spanner_admin_instance_v1.types.FieldMask]): Specifies the identities requesting access for a Cloud Platform
                resource. ``members`` can have the following values:

                -  ``allUsers``: A special identifier that represents anyone who is on
                   the internet; with or without a Google account.

                -  ``allAuthenticatedUsers``: A special identifier that represents
                   anyone who is authenticated with a Google account or a service
                   account.

                -  ``user:{emailid}``: An email address that represents a specific
                   Google account. For example, ``alice@example.com`` .

                -  ``serviceAccount:{emailid}``: An email address that represents a
                   service account. For example,
                   ``my-other-app@appspot.gserviceaccount.com``.

                -  ``group:{emailid}``: An email address that represents a Google group.
                   For example, ``admins@example.com``.

                -  ``domain:{domain}``: The G Suite domain (primary) that represents all
                   the users of that domain. For example, ``google.com`` or
                   ``example.com``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_instance_v1.types.FieldMask`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.api_core.operation.Operation` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "update_instance" not in self._inner_api_calls:
            self._inner_api_calls[
                "update_instance"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.update_instance,
                default_retry=self._method_configs["UpdateInstance"].retry,
                default_timeout=self._method_configs["UpdateInstance"].timeout,
                client_info=self._client_info,
            )

        request = spanner_instance_admin_pb2.UpdateInstanceRequest(
            instance=instance, field_mask=field_mask
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("instance.name", instance.name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        operation = self._inner_api_calls["update_instance"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )
        return google.api_core.operation.from_gapic(
            operation,
            self.transport._operations_client,
            spanner_instance_admin_pb2.Instance,
            metadata_type=spanner_instance_admin_pb2.UpdateInstanceMetadata,
        )

    def delete_instance(
        self,
        name,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        An annotation that describes a resource reference, see
        ``ResourceReference``.

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> name = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> client.delete_instance(name)

        Args:
            name (str): If set, all the classes from the .proto file are wrapped in a single
                outer class with the given name. This applies to both Proto1 (equivalent
                to the old "--one_java_file" option) and Proto2 (where a .proto always
                translates to a single class, but you may want to explicitly choose the
                class name).
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "delete_instance" not in self._inner_api_calls:
            self._inner_api_calls[
                "delete_instance"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.delete_instance,
                default_retry=self._method_configs["DeleteInstance"].retry,
                default_timeout=self._method_configs["DeleteInstance"].timeout,
                client_info=self._client_info,
            )

        request = spanner_instance_admin_pb2.DeleteInstanceRequest(name=name)
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("name", name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        self._inner_api_calls["delete_instance"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def set_iam_policy(
        self,
        resource,
        policy,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        If type_name is set, this need not be set. If both this and
        type_name are set, this must be one of TYPE_ENUM, TYPE_MESSAGE or
        TYPE_GROUP.

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> # TODO: Initialize `resource`:
            >>> resource = ''
            >>>
            >>> # TODO: Initialize `policy`:
            >>> policy = {}
            >>>
            >>> response = client.set_iam_policy(resource, policy)

        Args:
            resource (str): REQUIRED: The resource for which the policy is being specified.
                See the operation documentation for the appropriate value for this field.
            policy (Union[dict, ~google.cloud.spanner_admin_instance_v1.types.Policy]): The name of the uninterpreted option. Each string represents a
                segment in a dot-separated name. is_extension is true iff a segment
                represents an extension (denoted with parentheses in options specs in
                .proto files). E.g.,{ ["foo", false], ["bar.baz", true], ["qux", false]
                } represents "foo.(bar.baz).qux".

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_instance_v1.types.Policy`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_instance_v1.types.Policy` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "set_iam_policy" not in self._inner_api_calls:
            self._inner_api_calls[
                "set_iam_policy"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.set_iam_policy,
                default_retry=self._method_configs["SetIamPolicy"].retry,
                default_timeout=self._method_configs["SetIamPolicy"].timeout,
                client_info=self._client_info,
            )

        request = iam_policy_pb2.SetIamPolicyRequest(resource=resource, policy=policy)
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("resource", resource)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["set_iam_policy"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def get_iam_policy(
        self,
        resource,
        options_=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Denotes a field as output only. This indicates that the field is
        provided in responses, but including the field in a request does nothing
        (the server *must* ignore it and *must not* throw an error as a result
        of the field's presence).

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> # TODO: Initialize `resource`:
            >>> resource = ''
            >>>
            >>> response = client.get_iam_policy(resource)

        Args:
            resource (str): REQUIRED: The resource for which the policy is being requested.
                See the operation documentation for the appropriate value for this field.
            options_ (Union[dict, ~google.cloud.spanner_admin_instance_v1.types.GetPolicyOptions]): Updates an instance, and begins allocating or releasing resources as
                requested. The returned ``long-running operation`` can be used to track
                the progress of updating the instance. If the named instance does not
                exist, returns ``NOT_FOUND``.

                Immediately upon completion of this request:

                -  For resource types for which a decrease in the instance's allocation
                   has been requested, billing is based on the newly-requested level.

                Until completion of the returned operation:

                -  Cancelling the operation sets its metadata's ``cancel_time``, and
                   begins restoring resources to their pre-request values. The operation
                   is guaranteed to succeed at undoing all resource changes, after which
                   point it terminates with a ``CANCELLED`` status.
                -  All other attempts to modify the instance are rejected.
                -  Reading the instance via the API continues to give the pre-request
                   resource levels.

                Upon completion of the returned operation:

                -  Billing begins for all successfully-allocated resources (some types
                   may have lower than the requested levels).
                -  All newly-reserved resources are available for serving the instance's
                   tables.
                -  The instance's new resource levels are readable via the API.

                The returned ``long-running operation`` will have a name of the format
                ``<instance_name>/operations/<operation_id>`` and can be used to track
                the instance modification. The ``metadata`` field type is
                ``UpdateInstanceMetadata``. The ``response`` field type is ``Instance``,
                if successful.

                Authorization requires ``spanner.instances.update`` permission on
                resource ``name``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_instance_v1.types.GetPolicyOptions`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_instance_v1.types.Policy` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "get_iam_policy" not in self._inner_api_calls:
            self._inner_api_calls[
                "get_iam_policy"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_iam_policy,
                default_retry=self._method_configs["GetIamPolicy"].retry,
                default_timeout=self._method_configs["GetIamPolicy"].timeout,
                client_info=self._client_info,
            )

        request = iam_policy_pb2.GetIamPolicyRequest(
            resource=resource, options=options_
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("resource", resource)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["get_iam_policy"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def test_iam_permissions(
        self,
        resource,
        permissions,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        The server-assigned name, which is only unique within the same
        service that originally returns it. If you use the default HTTP mapping,
        the ``name`` should be a resource name ending with
        ``operations/{unique_id}``.

        Example:
            >>> from google.cloud import spanner_admin_instance_v1
            >>>
            >>> client = spanner_admin_instance_v1.InstanceAdminClient()
            >>>
            >>> # TODO: Initialize `resource`:
            >>> resource = ''
            >>>
            >>> # TODO: Initialize `permissions`:
            >>> permissions = []
            >>>
            >>> response = client.test_iam_permissions(resource, permissions)

        Args:
            resource (str): REQUIRED: The resource for which the policy detail is being requested.
                See the operation documentation for the appropriate value for this field.
            permissions (list[str]): Metadata type for the operation returned by ``CreateInstance``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_instance_v1.types.TestIamPermissionsResponse` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "test_iam_permissions" not in self._inner_api_calls:
            self._inner_api_calls[
                "test_iam_permissions"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.test_iam_permissions,
                default_retry=self._method_configs["TestIamPermissions"].retry,
                default_timeout=self._method_configs["TestIamPermissions"].timeout,
                client_info=self._client_info,
            )

        request = iam_policy_pb2.TestIamPermissionsRequest(
            resource=resource, permissions=permissions
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("resource", resource)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["test_iam_permissions"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )
