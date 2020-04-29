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

"""Accesses the google.spanner.admin.database.v1 DatabaseAdmin API."""

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
import google.api_core.protobuf_helpers
import grpc

from google.cloud.spanner_admin_database_v1.gapic import database_admin_client_config
from google.cloud.spanner_admin_database_v1.gapic import enums
from google.cloud.spanner_admin_database_v1.gapic.transports import (
    database_admin_grpc_transport,
)
from google.cloud.spanner_admin_database_v1.proto import backup_pb2
from google.cloud.spanner_admin_database_v1.proto import spanner_database_admin_pb2
from google.cloud.spanner_admin_database_v1.proto import spanner_database_admin_pb2_grpc
from google.iam.v1 import iam_policy_pb2
from google.iam.v1 import options_pb2
from google.iam.v1 import policy_pb2
from google.longrunning import operations_pb2
from google.protobuf import empty_pb2
from google.protobuf import field_mask_pb2


_GAPIC_LIBRARY_VERSION = pkg_resources.get_distribution("google-cloud-spanner").version


class DatabaseAdminClient(object):
    """
    Cloud Spanner Database Admin API

    The Cloud Spanner Database Admin API can be used to create, drop, and
    list databases. It also enables updating the schema of pre-existing
    databases. It can be also used to create, delete and list backups for a
    database and to restore from an existing backup.
    """

    SERVICE_ADDRESS = "spanner.googleapis.com:443"
    """The default address of the service."""

    # The name of the interface for this client. This is the key used to
    # find the method configuration in the client_config dictionary.
    _INTERFACE_NAME = "google.spanner.admin.database.v1.DatabaseAdmin"

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
            DatabaseAdminClient: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_file(filename)
        kwargs["credentials"] = credentials
        return cls(*args, **kwargs)

    from_service_account_json = from_service_account_file

    @classmethod
    def backup_path(cls, project, instance, backup):
        """Return a fully-qualified backup string."""
        return google.api_core.path_template.expand(
            "projects/{project}/instances/{instance}/backups/{backup}",
            project=project,
            instance=instance,
            backup=backup,
        )

    @classmethod
    def database_path(cls, project, instance, database):
        """Return a fully-qualified database string."""
        return google.api_core.path_template.expand(
            "projects/{project}/instances/{instance}/databases/{database}",
            project=project,
            instance=instance,
            database=database,
        )

    @classmethod
    def instance_path(cls, project, instance):
        """Return a fully-qualified instance string."""
        return google.api_core.path_template.expand(
            "projects/{project}/instances/{instance}",
            project=project,
            instance=instance,
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
            transport (Union[~.DatabaseAdminGrpcTransport,
                    Callable[[~.Credentials, type], ~.DatabaseAdminGrpcTransport]): A transport
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
            client_config = database_admin_client_config.config

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
                    default_class=database_admin_grpc_transport.DatabaseAdminGrpcTransport,
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
            self.transport = database_admin_grpc_transport.DatabaseAdminGrpcTransport(
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
    def create_database(
        self,
        parent,
        create_statement,
        extra_statements=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Denotes a field as required. This indicates that the field **must**
        be provided as part of the request, and failure to do so will cause an
        error (usually ``INVALID_ARGUMENT``).

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> parent = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> # TODO: Initialize `create_statement`:
            >>> create_statement = ''
            >>>
            >>> response = client.create_database(parent, create_statement)
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
            parent (str): Protocol Buffers - Google's data interchange format Copyright 2008
                Google Inc. All rights reserved.
                https://developers.google.com/protocol-buffers/

                Redistribution and use in source and binary forms, with or without
                modification, are permitted provided that the following conditions are
                met:

                ::

                    * Redistributions of source code must retain the above copyright

                notice, this list of conditions and the following disclaimer. \*
                Redistributions in binary form must reproduce the above copyright
                notice, this list of conditions and the following disclaimer in the
                documentation and/or other materials provided with the distribution. \*
                Neither the name of Google Inc. nor the names of its contributors may be
                used to endorse or promote products derived from this software without
                specific prior written permission.

                THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
                IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
                TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
                PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
                OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
                EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
                PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
                PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
                LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
                NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
                SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
            create_statement (str): The backup contains an externally consistent copy of
                ``source_database`` at the timestamp specified by ``create_time``.
            extra_statements (list[str]): Optional. A list of DDL statements to run inside the newly created
                database. Statements can create tables, indexes, etc. These
                statements execute atomically with the creation of the database:
                if there is an error in any statement, the database is not created.
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
        if "create_database" not in self._inner_api_calls:
            self._inner_api_calls[
                "create_database"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.create_database,
                default_retry=self._method_configs["CreateDatabase"].retry,
                default_timeout=self._method_configs["CreateDatabase"].timeout,
                client_info=self._client_info,
            )

        request = spanner_database_admin_pb2.CreateDatabaseRequest(
            parent=parent,
            create_statement=create_statement,
            extra_statements=extra_statements,
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

        operation = self._inner_api_calls["create_database"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )
        return google.api_core.operation.from_gapic(
            operation,
            self.transport._operations_client,
            spanner_database_admin_pb2.Database,
            metadata_type=spanner_database_admin_pb2.CreateDatabaseMetadata,
        )

    def get_database(
        self,
        name,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Gets the state of a Cloud Spanner database.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> name = client.database_path('[PROJECT]', '[INSTANCE]', '[DATABASE]')
            >>>
            >>> response = client.get_database(name)

        Args:
            name (str): Required. The instance whose databases should be listed. Values are
                of the form ``projects/<project>/instances/<instance>``.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_database_v1.types.Database` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "get_database" not in self._inner_api_calls:
            self._inner_api_calls[
                "get_database"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_database,
                default_retry=self._method_configs["GetDatabase"].retry,
                default_timeout=self._method_configs["GetDatabase"].timeout,
                client_info=self._client_info,
            )

        request = spanner_database_admin_pb2.GetDatabaseRequest(name=name)
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

        return self._inner_api_calls["get_database"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def update_database_ddl(
        self,
        database,
        statements,
        operation_id=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        An expression that filters the list of returned backup operations.

        A filter expression consists of a field name, a comparison operator, and
        a value for filtering. The value must be a string, a number, or a
        boolean. The comparison operator must be one of: ``<``, ``>``, ``<=``,
        ``>=``, ``!=``, ``=``, or ``:``. Colon ``:`` is the contains operator.
        Filter rules are not case sensitive.

        The following fields in the ``operation`` are eligible for filtering:

        -  ``name`` - The name of the long-running operation
        -  ``done`` - False if the operation is in progress, else true.
        -  ``metadata.@type`` - the type of metadata. For example, the type
           string for ``CreateBackupMetadata`` is
           ``type.googleapis.com/google.spanner.admin.database.v1.CreateBackupMetadata``.
        -  ``metadata.<field_name>`` - any field in metadata.value.
        -  ``error`` - Error associated with the long-running operation.
        -  ``response.@type`` - the type of response.
        -  ``response.<field_name>`` - any field in response.value.

        You can combine multiple expressions by enclosing each expression in
        parentheses. By default, expressions are combined with AND logic, but
        you can specify AND, OR, and NOT logic explicitly.

        Here are a few examples:

        -  ``done:true`` - The operation is complete.
        -  ``metadata.database:prod`` - The database the backup was taken from
           has a name containing the string "prod".
        -  ``(metadata.@type=type.googleapis.com/google.spanner.admin.database.v1.CreateBackupMetadata) AND``
           ``(metadata.name:howl) AND``
           ``(metadata.progress.start_time < \"2018-03-28T14:50:00Z\") AND``
           ``(error:*)`` - Returns operations where:

           -  The operation's metadata type is ``CreateBackupMetadata``.
           -  The backup name contains the string "howl".
           -  The operation started before 2018-03-28T14:50:00Z.
           -  The operation resulted in an error.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> database = client.database_path('[PROJECT]', '[INSTANCE]', '[DATABASE]')
            >>>
            >>> # TODO: Initialize `statements`:
            >>> statements = []
            >>>
            >>> response = client.update_database_ddl(database, statements)
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
            database (str): Required. The database to update.
            statements (list[str]): Required. DDL statements to be applied to the database.
            operation_id (str): OPTIONAL: A ``GetPolicyOptions`` object for specifying options to
                ``GetIamPolicy``. This field is only used by Cloud IAM.
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
        if "update_database_ddl" not in self._inner_api_calls:
            self._inner_api_calls[
                "update_database_ddl"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.update_database_ddl,
                default_retry=self._method_configs["UpdateDatabaseDdl"].retry,
                default_timeout=self._method_configs["UpdateDatabaseDdl"].timeout,
                client_info=self._client_info,
            )

        request = spanner_database_admin_pb2.UpdateDatabaseDdlRequest(
            database=database, statements=statements, operation_id=operation_id
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("database", database)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        operation = self._inner_api_calls["update_database_ddl"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )
        return google.api_core.operation.from_gapic(
            operation,
            self.transport._operations_client,
            empty_pb2.Empty,
            metadata_type=spanner_database_admin_pb2.UpdateDatabaseDdlMetadata,
        )

    def drop_database(
        self,
        database,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        ``FieldMask`` represents a set of symbolic field paths, for example:

        ::

            paths: "f.a"
            paths: "f.b.d"

        Here ``f`` represents a field in some root message, ``a`` and ``b``
        fields in the message found in ``f``, and ``d`` a field found in the
        message in ``f.b``.

        Field masks are used to specify a subset of fields that should be
        returned by a get operation or modified by an update operation. Field
        masks also have a custom JSON encoding (see below).

        # Field Masks in Projections

        When used in the context of a projection, a response message or
        sub-message is filtered by the API to only contain those fields as
        specified in the mask. For example, if the mask in the previous example
        is applied to a response message as follows:

        ::

            f {
              a : 22
              b {
                d : 1
                x : 2
              }
              y : 13
            }
            z: 8

        The result will not contain specific values for fields x,y and z (their
        value will be set to the default, and omitted in proto text output):

        ::

            f {
              a : 22
              b {
                d : 1
              }
            }

        A repeated field is not allowed except at the last position of a paths
        string.

        If a FieldMask object is not present in a get operation, the operation
        applies to all fields (as if a FieldMask of all fields had been
        specified).

        Note that a field mask does not necessarily apply to the top-level
        response message. In case of a REST get operation, the field mask
        applies directly to the response, but in case of a REST list operation,
        the mask instead applies to each individual message in the returned
        resource list. In case of a REST custom method, other definitions may be
        used. Where the mask applies will be clearly documented together with
        its declaration in the API. In any case, the effect on the returned
        resource/resources is required behavior for APIs.

        # Field Masks in Update Operations

        A field mask in update operations specifies which fields of the targeted
        resource are going to be updated. The API is required to only change the
        values of the fields as specified in the mask and leave the others
        untouched. If a resource is passed in to describe the updated values,
        the API ignores the values of all fields not covered by the mask.

        If a repeated field is specified for an update operation, new values
        will be appended to the existing repeated field in the target resource.
        Note that a repeated field is only allowed in the last position of a
        ``paths`` string.

        If a sub-message is specified in the last position of the field mask for
        an update operation, then new value will be merged into the existing
        sub-message in the target resource.

        For example, given the target message:

        ::

            f {
              b {
                d: 1
                x: 2
              }
              c: [1]
            }

        And an update message:

        ::

            f {
              b {
                d: 10
              }
              c: [2]
            }

        then if the field mask is:

        paths: ["f.b", "f.c"]

        then the result will be:

        ::

            f {
              b {
                d: 10
                x: 2
              }
              c: [1, 2]
            }

        An implementation may provide options to override this default behavior
        for repeated and message fields.

        In order to reset a field's value to the default, the field must be in
        the mask and set to the default value in the provided resource. Hence,
        in order to reset all fields of a resource, provide a default instance
        of the resource and set all fields in the mask, or do not provide a mask
        as described below.

        If a field mask is not present on update, the operation applies to all
        fields (as if a field mask of all fields has been specified). Note that
        in the presence of schema evolution, this may mean that fields the
        client does not know and has therefore not filled into the request will
        be reset to their default. If this is unwanted behavior, a specific
        service may require a client to always specify a field mask, producing
        an error if not.

        As with get operations, the location of the resource which describes the
        updated values in the request message depends on the operation kind. In
        any case, the effect of the field mask is required to be honored by the
        API.

        ## Considerations for HTTP REST

        The HTTP kind of an update operation which uses a field mask must be set
        to PATCH instead of PUT in order to satisfy HTTP semantics (PUT must
        only be used for full updates).

        # JSON Encoding of Field Masks

        In JSON, a field mask is encoded as a single string where paths are
        separated by a comma. Fields name in each path are converted to/from
        lower-camel naming conventions.

        As an example, consider the following message declarations:

        ::

            message Profile {
              User user = 1;
              Photo photo = 2;
            }
            message User {
              string display_name = 1;
              string address = 2;
            }

        In proto a field mask for ``Profile`` may look as such:

        ::

            mask {
              paths: "user.display_name"
              paths: "photo"
            }

        In JSON, the same mask is represented as below:

        ::

            {
              mask: "user.displayName,photo"
            }

        # Field Masks and Oneof Fields

        Field masks treat fields in oneofs just as regular fields. Consider the
        following message:

        ::

            message SampleMessage {
              oneof test_oneof {
                string name = 4;
                SubMessage sub_message = 9;
              }
            }

        The field mask can be:

        ::

            mask {
              paths: "name"
            }

        Or:

        ::

            mask {
              paths: "sub_message"
            }

        Note that oneof type names ("test_oneof" in this case) cannot be used in
        paths.

        ## Field Mask Verification

        The implementation of any API method which has a FieldMask type field in
        the request should verify the included field paths, and return an
        ``INVALID_ARGUMENT`` error if any path is unmappable.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> database = client.database_path('[PROJECT]', '[INSTANCE]', '[DATABASE]')
            >>>
            >>> client.drop_database(database)

        Args:
            database (str): Required. The database to be dropped.
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
        if "drop_database" not in self._inner_api_calls:
            self._inner_api_calls[
                "drop_database"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.drop_database,
                default_retry=self._method_configs["DropDatabase"].retry,
                default_timeout=self._method_configs["DropDatabase"].timeout,
                client_info=self._client_info,
            )

        request = spanner_database_admin_pb2.DropDatabaseRequest(database=database)
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("database", database)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        self._inner_api_calls["drop_database"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def get_database_ddl(
        self,
        database,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Protocol Buffers - Google's data interchange format Copyright 2008
        Google Inc. All rights reserved.
        https://developers.google.com/protocol-buffers/

        Redistribution and use in source and binary forms, with or without
        modification, are permitted provided that the following conditions are
        met:

        ::

            * Redistributions of source code must retain the above copyright

        notice, this list of conditions and the following disclaimer. \*
        Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution. \*
        Neither the name of Google Inc. nor the names of its contributors may be
        used to endorse or promote products derived from this software without
        specific prior written permission.

        THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
        IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
        TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
        PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
        OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
        EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
        PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
        PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
        LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
        NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
        SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> database = client.database_path('[PROJECT]', '[INSTANCE]', '[DATABASE]')
            >>>
            >>> response = client.get_database_ddl(database)

        Args:
            database (str): Required. The database whose schema we wish to get.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_database_v1.types.GetDatabaseDdlResponse` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "get_database_ddl" not in self._inner_api_calls:
            self._inner_api_calls[
                "get_database_ddl"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_database_ddl,
                default_retry=self._method_configs["GetDatabaseDdl"].retry,
                default_timeout=self._method_configs["GetDatabaseDdl"].timeout,
                client_info=self._client_info,
            )

        request = spanner_database_admin_pb2.GetDatabaseDdlRequest(database=database)
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("database", database)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["get_database_ddl"](
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
        Denotes a field as output only. This indicates that the field is
        provided in responses, but including the field in a request does nothing
        (the server *must* ignore it and *must not* throw an error as a result
        of the field's presence).

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
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
            policy (Union[dict, ~google.cloud.spanner_admin_database_v1.types.Policy]): ``Any`` contains an arbitrary serialized protocol buffer message
                along with a URL that describes the type of the serialized message.

                Protobuf library provides support to pack/unpack Any values in the form
                of utility functions or additional generated methods of the Any type.

                Example 1: Pack and unpack a message in C++.

                ::

                    Foo foo = ...;
                    Any any;
                    any.PackFrom(foo);
                    ...
                    if (any.UnpackTo(&foo)) {
                      ...
                    }

                Example 2: Pack and unpack a message in Java.

                ::

                    Foo foo = ...;
                    Any any = Any.pack(foo);
                    ...
                    if (any.is(Foo.class)) {
                      foo = any.unpack(Foo.class);
                    }

                Example 3: Pack and unpack a message in Python.

                ::

                    foo = Foo(...)
                    any = Any()
                    any.Pack(foo)
                    ...
                    if any.Is(Foo.DESCRIPTOR):
                      any.Unpack(foo)
                      ...

                Example 4: Pack and unpack a message in Go

                ::

                     foo := &pb.Foo{...}
                     any, err := ptypes.MarshalAny(foo)
                     ...
                     foo := &pb.Foo{}
                     if err := ptypes.UnmarshalAny(any, foo); err != nil {
                       ...
                     }

                The pack methods provided by protobuf library will by default use
                'type.googleapis.com/full.type.name' as the type URL and the unpack
                methods only use the fully qualified type name after the last '/' in the
                type URL, for example "foo.bar.com/x/y.z" will yield type name "y.z".

                # JSON

                The JSON representation of an ``Any`` value uses the regular
                representation of the deserialized, embedded message, with an additional
                field ``@type`` which contains the type URL. Example:

                ::

                    package google.profile;
                    message Person {
                      string first_name = 1;
                      string last_name = 2;
                    }

                    {
                      "@type": "type.googleapis.com/google.profile.Person",
                      "firstName": <string>,
                      "lastName": <string>
                    }

                If the embedded message type is well-known and has a custom JSON
                representation, that representation will be embedded adding a field
                ``value`` which holds the custom JSON in addition to the ``@type``
                field. Example (for message ``google.protobuf.Duration``):

                ::

                    {
                      "@type": "type.googleapis.com/google.protobuf.Duration",
                      "value": "1.212s"
                    }

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_database_v1.types.Policy`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_database_v1.types.Policy` instance.

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
        Request message for ``SetIamPolicy`` method.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> # TODO: Initialize `resource`:
            >>> resource = ''
            >>>
            >>> response = client.get_iam_policy(resource)

        Args:
            resource (str): REQUIRED: The resource for which the policy is being requested.
                See the operation documentation for the appropriate value for this field.
            options_ (Union[dict, ~google.cloud.spanner_admin_database_v1.types.GetPolicyOptions]): The request for ``ListBackupOperations``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_database_v1.types.GetPolicyOptions`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_database_v1.types.Policy` instance.

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
        The database is fully created and ready for use, but is still being
        optimized for performance and cannot handle full load.

        In this state, the database still references the backup it was restore
        from, preventing the backup from being deleted. When optimizations are
        complete, the full performance of the database will be restored, and the
        database will transition to ``READY`` state.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
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
            permissions (list[str]): If set true, then the Java code generator will generate a separate
                .java file for each top-level message, enum, and service defined in the
                .proto file. Thus, these types will *not* be nested inside the outer
                class named by java_outer_classname. However, the outer class will still
                be generated to contain the file's getDescriptor() method as well as any
                top-level extensions defined in the file.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_database_v1.types.TestIamPermissionsResponse` instance.

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

    def create_backup(
        self,
        parent,
        backup_id,
        backup,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Required. The name of the database. Values are of the form
        ``projects/<project>/instances/<instance>/databases/<database>``, where
        ``<database>`` is as specified in the ``CREATE DATABASE`` statement.
        This name can be passed to other API methods to identify the database.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> parent = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> # TODO: Initialize `backup_id`:
            >>> backup_id = ''
            >>>
            >>> # TODO: Initialize `backup`:
            >>> backup = {}
            >>>
            >>> response = client.create_backup(parent, backup_id, backup)
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
            parent (str): Required. The name of the instance in which to create the restored
                database. This instance must be in the same project and have the same
                instance configuration as the instance containing the source backup.
                Values are of the form ``projects/<project>/instances/<instance>``.
            backup_id (str): The response message for ``Operations.ListOperations``.
            backup (Union[dict, ~google.cloud.spanner_admin_database_v1.types.Backup]): Required. The backup to create.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_database_v1.types.Backup`
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
        if "create_backup" not in self._inner_api_calls:
            self._inner_api_calls[
                "create_backup"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.create_backup,
                default_retry=self._method_configs["CreateBackup"].retry,
                default_timeout=self._method_configs["CreateBackup"].timeout,
                client_info=self._client_info,
            )

        request = backup_pb2.CreateBackupRequest(
            parent=parent, backup_id=backup_id, backup=backup
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

        operation = self._inner_api_calls["create_backup"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )
        return google.api_core.operation.from_gapic(
            operation,
            self.transport._operations_client,
            backup_pb2.Backup,
            metadata_type=backup_pb2.CreateBackupMetadata,
        )

    def get_backup(
        self,
        name,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        If non-empty, ``page_token`` should contain a ``next_page_token``
        from a previous ``ListBackupOperationsResponse`` to the same ``parent``
        and with the same ``filter``.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> name = client.backup_path('[PROJECT]', '[INSTANCE]', '[BACKUP]')
            >>>
            >>> response = client.get_backup(name)

        Args:
            name (str): Create a new database by restoring from a completed backup. The new
                database must be in the same project and in an instance with the same
                instance configuration as the instance containing the backup. The
                returned database ``long-running operation`` has a name of the format
                ``projects/<project>/instances/<instance>/databases/<database>/operations/<operation_id>``,
                and can be used to track the progress of the operation, and to cancel
                it. The ``metadata`` field type is ``RestoreDatabaseMetadata``. The
                ``response`` type is ``Database``, if successful. Cancelling the
                returned operation will stop the restore and delete the database. There
                can be only one database being restored into an instance at a time. Once
                the restore operation completes, a new restore operation can be
                initiated, without waiting for the optimize operation associated with
                the first restore to complete.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_database_v1.types.Backup` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "get_backup" not in self._inner_api_calls:
            self._inner_api_calls[
                "get_backup"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_backup,
                default_retry=self._method_configs["GetBackup"].retry,
                default_timeout=self._method_configs["GetBackup"].timeout,
                client_info=self._client_info,
            )

        request = backup_pb2.GetBackupRequest(name=name)
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

        return self._inner_api_calls["get_backup"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def update_backup(
        self,
        backup,
        update_mask,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Not ZigZag encoded. Negative numbers take 10 bytes. Use TYPE_SINT64
        if negative values are likely.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> # TODO: Initialize `backup`:
            >>> backup = {}
            >>>
            >>> # TODO: Initialize `update_mask`:
            >>> update_mask = {}
            >>>
            >>> response = client.update_backup(backup, update_mask)

        Args:
            backup (Union[dict, ~google.cloud.spanner_admin_database_v1.types.Backup]): Specifies a service that was configured for Cloud Audit Logging. For
                example, ``storage.googleapis.com``, ``cloudsql.googleapis.com``.
                ``allServices`` is a special value that covers all services. Required

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_database_v1.types.Backup`
            update_mask (Union[dict, ~google.cloud.spanner_admin_database_v1.types.FieldMask]): Information about the source used to restore the database, as
                specified by ``source`` in ``RestoreDatabaseRequest``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_admin_database_v1.types.FieldMask`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_admin_database_v1.types.Backup` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "update_backup" not in self._inner_api_calls:
            self._inner_api_calls[
                "update_backup"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.update_backup,
                default_retry=self._method_configs["UpdateBackup"].retry,
                default_timeout=self._method_configs["UpdateBackup"].timeout,
                client_info=self._client_info,
            )

        request = backup_pb2.UpdateBackupRequest(backup=backup, update_mask=update_mask)
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("backup.name", backup.name)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["update_backup"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def delete_backup(
        self,
        name,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Protocol Buffers - Google's data interchange format Copyright 2008
        Google Inc. All rights reserved.
        https://developers.google.com/protocol-buffers/

        Redistribution and use in source and binary forms, with or without
        modification, are permitted provided that the following conditions are
        met:

        ::

            * Redistributions of source code must retain the above copyright

        notice, this list of conditions and the following disclaimer. \*
        Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution. \*
        Neither the name of Google Inc. nor the names of its contributors may be
        used to endorse or promote products derived from this software without
        specific prior written permission.

        THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
        IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
        TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
        PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
        OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
        EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
        PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
        PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
        LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
        NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
        SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> name = client.backup_path('[PROJECT]', '[INSTANCE]', '[BACKUP]')
            >>>
            >>> client.delete_backup(name)

        Args:
            name (str): The request message for ``Operations.CancelOperation``.
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
        if "delete_backup" not in self._inner_api_calls:
            self._inner_api_calls[
                "delete_backup"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.delete_backup,
                default_retry=self._method_configs["DeleteBackup"].retry,
                default_timeout=self._method_configs["DeleteBackup"].timeout,
                client_info=self._client_info,
            )

        request = backup_pb2.DeleteBackupRequest(name=name)
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

        self._inner_api_calls["delete_backup"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def list_backups(
        self,
        parent,
        filter_=None,
        page_size=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        REQUIRED: The complete policy to be applied to the ``resource``. The
        size of the policy is limited to a few 10s of KB. An empty policy is a
        valid policy but certain Cloud Platform services (such as Projects)
        might reject them.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> parent = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_backups(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_backups(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): Specifies the log_type that was be enabled. ADMIN_ACTIVITY is always
                enabled, and cannot be configured. Required
            filter_ (str): Input and output type names. These are resolved in the same way as
                FieldDescriptorProto.type_name, but must refer to a message type.
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
            An iterable of :class:`~google.cloud.spanner_admin_database_v1.types.Backup` instances.
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
        if "list_backups" not in self._inner_api_calls:
            self._inner_api_calls[
                "list_backups"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_backups,
                default_retry=self._method_configs["ListBackups"].retry,
                default_timeout=self._method_configs["ListBackups"].timeout,
                client_info=self._client_info,
            )

        request = backup_pb2.ListBackupsRequest(
            parent=parent, filter=filter_, page_size=page_size
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
                self._inner_api_calls["list_backups"],
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            ),
            request=request,
            items_field="backups",
            request_token_field="page_token",
            response_token_field="next_page_token",
        )
        return iterator

    def restore_database(
        self,
        parent,
        database_id,
        backup=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        The response for ``ListBackupOperations``.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> parent = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> # TODO: Initialize `database_id`:
            >>> database_id = ''
            >>>
            >>> response = client.restore_database(parent, database_id)
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
            parent (str): The resource type. It must be in the format of
                {service_name}/{resource_type_kind}. The ``resource_type_kind`` must be
                singular and must not include version numbers.

                Example: ``storage.googleapis.com/Bucket``

                The value of the resource_type_kind must follow the regular expression
                /[A-Za-z][a-zA-Z0-9]+/. It should start with an upper case character and
                should use PascalCase (UpperCamelCase). The maximum number of characters
                allowed for the ``resource_type_kind`` is 100.
            database_id (str): Required. The name of the instance that will serve the new database.
                Values are of the form ``projects/<project>/instances/<instance>``.
            backup (str): The ``Status`` type defines a logical error model that is suitable
                for different programming environments, including REST APIs and RPC
                APIs. It is used by `gRPC <https://github.com/grpc>`__. Each ``Status``
                message contains three pieces of data: error code, error message, and
                error details.

                You can find out more about this error model and how to work with it in
                the `API Design Guide <https://cloud.google.com/apis/design/errors>`__.
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
        if "restore_database" not in self._inner_api_calls:
            self._inner_api_calls[
                "restore_database"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.restore_database,
                default_retry=self._method_configs["RestoreDatabase"].retry,
                default_timeout=self._method_configs["RestoreDatabase"].timeout,
                client_info=self._client_info,
            )

        # Sanity check: We have some fields which are mutually exclusive;
        # raise ValueError if more than one is sent.
        google.api_core.protobuf_helpers.check_oneof(backup=backup)

        request = spanner_database_admin_pb2.RestoreDatabaseRequest(
            parent=parent, database_id=database_id, backup=backup
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

        operation = self._inner_api_calls["restore_database"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )
        return google.api_core.operation.from_gapic(
            operation,
            self.transport._operations_client,
            spanner_database_admin_pb2.Database,
            metadata_type=spanner_database_admin_pb2.RestoreDatabaseMetadata,
        )

    def list_database_operations(
        self,
        parent,
        filter_=None,
        page_size=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        The list of matching backup ``long-running operations``. Each
        operation's name will be prefixed by the backup's name and the
        operation's ``metadata`` will be of type ``CreateBackupMetadata``.
        Operations returned include those that are pending or have
        completed/failed/canceled within the last 7 days. Operations returned
        are ordered by ``operation.metadata.value.progress.start_time`` in
        descending order starting from the most recently started operation.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> parent = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_database_operations(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_database_operations(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): Response message for ``TestIamPermissions`` method.
            filter_ (str): The response for ``ListDatabases``.
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
            An iterable of :class:`~google.cloud.spanner_admin_database_v1.types.Operation` instances.
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
        if "list_database_operations" not in self._inner_api_calls:
            self._inner_api_calls[
                "list_database_operations"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_database_operations,
                default_retry=self._method_configs["ListDatabaseOperations"].retry,
                default_timeout=self._method_configs["ListDatabaseOperations"].timeout,
                client_info=self._client_info,
            )

        request = spanner_database_admin_pb2.ListDatabaseOperationsRequest(
            parent=parent, filter=filter_, page_size=page_size
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
                self._inner_api_calls["list_database_operations"],
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            ),
            request=request,
            items_field="operations",
            request_token_field="page_token",
            response_token_field="next_page_token",
        )
        return iterator

    def list_backup_operations(
        self,
        parent,
        filter_=None,
        page_size=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Not ZigZag encoded. Negative numbers take 10 bytes. Use TYPE_SINT32
        if negative values are likely.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> parent = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_backup_operations(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_backup_operations(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): Required. The instance to list backups from. Values are of the form
                ``projects/<project>/instances/<instance>``.
            filter_ (str): Should this field be parsed lazily? Lazy applies only to
                message-type fields. It means that when the outer message is initially
                parsed, the inner message's contents will not be parsed but instead
                stored in encoded form. The inner message will actually be parsed when
                it is first accessed.

                This is only a hint. Implementations are free to choose whether to use
                eager or lazy parsing regardless of the value of this option. However,
                setting this option true suggests that the protocol author believes that
                using lazy parsing on this field is worth the additional bookkeeping
                overhead typically needed to implement it.

                This option does not affect the public interface of any generated code;
                all method signatures remain the same. Furthermore, thread-safety of the
                interface is not affected by this option; const methods remain safe to
                call from multiple threads concurrently, while non-const methods
                continue to require exclusive access.

                Note that implementations may choose not to check required fields within
                a lazy sub-message. That is, calling IsInitialized() on the outer
                message may return true even if the inner message has missing required
                fields. This is necessary because otherwise the inner message would have
                to be parsed in order to perform the check, defeating the purpose of
                lazy parsing. An implementation which chooses not to check required
                fields must be consistent about it. That is, for any particular
                sub-message, the implementation must either *always* check its required
                fields, or *never* check its required fields, regardless of whether or
                not the message has been parsed.
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
            An iterable of :class:`~google.cloud.spanner_admin_database_v1.types.Operation` instances.
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
        if "list_backup_operations" not in self._inner_api_calls:
            self._inner_api_calls[
                "list_backup_operations"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_backup_operations,
                default_retry=self._method_configs["ListBackupOperations"].retry,
                default_timeout=self._method_configs["ListBackupOperations"].timeout,
                client_info=self._client_info,
            )

        request = backup_pb2.ListBackupOperationsRequest(
            parent=parent, filter=filter_, page_size=page_size
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
                self._inner_api_calls["list_backup_operations"],
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            ),
            request=request,
            items_field="operations",
            request_token_field="page_token",
            response_token_field="next_page_token",
        )
        return iterator

    def list_databases(
        self,
        parent,
        page_size=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Lists Cloud Spanner databases.

        Example:
            >>> from google.cloud import spanner_admin_database_v1
            >>>
            >>> client = spanner_admin_database_v1.DatabaseAdminClient()
            >>>
            >>> parent = client.instance_path('[PROJECT]', '[INSTANCE]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_databases(parent):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_databases(parent).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            parent (str): javanano_as_lite
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
            An iterable of :class:`~google.cloud.spanner_admin_database_v1.types.Database` instances.
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
        if "list_databases" not in self._inner_api_calls:
            self._inner_api_calls[
                "list_databases"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_databases,
                default_retry=self._method_configs["ListDatabases"].retry,
                default_timeout=self._method_configs["ListDatabases"].timeout,
                client_info=self._client_info,
            )

        request = spanner_database_admin_pb2.ListDatabasesRequest(
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
                self._inner_api_calls["list_databases"],
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            ),
            request=request,
            items_field="databases",
            request_token_field="page_token",
            response_token_field="next_page_token",
        )
        return iterator
