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

"""Accesses the google.spanner.v1 Spanner API."""

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
import google.api_core.page_iterator
import google.api_core.path_template
import google.api_core.protobuf_helpers
import grpc

from google.cloud.spanner_v1.gapic import enums
from google.cloud.spanner_v1.gapic import spanner_client_config
from google.cloud.spanner_v1.gapic.transports import spanner_grpc_transport
from google.cloud.spanner_v1.proto import keys_pb2
from google.cloud.spanner_v1.proto import mutation_pb2
from google.cloud.spanner_v1.proto import result_set_pb2
from google.cloud.spanner_v1.proto import spanner_pb2
from google.cloud.spanner_v1.proto import spanner_pb2_grpc
from google.cloud.spanner_v1.proto import transaction_pb2
from google.protobuf import empty_pb2
from google.protobuf import struct_pb2


_GAPIC_LIBRARY_VERSION = pkg_resources.get_distribution("google-cloud-spanner").version


class SpannerClient(object):
    """
    Cloud Spanner API

    The Cloud Spanner API can be used to manage sessions and execute
    transactions on data stored in Cloud Spanner databases.
    """

    SERVICE_ADDRESS = "spanner.googleapis.com:443"
    """The default address of the service."""

    # The name of the interface for this client. This is the key used to
    # find the method configuration in the client_config dictionary.
    _INTERFACE_NAME = "google.spanner.v1.Spanner"

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
            SpannerClient: The constructed client.
        """
        credentials = service_account.Credentials.from_service_account_file(filename)
        kwargs["credentials"] = credentials
        return cls(*args, **kwargs)

    from_service_account_json = from_service_account_file

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
    def session_path(cls, project, instance, database, session):
        """Return a fully-qualified session string."""
        return google.api_core.path_template.expand(
            "projects/{project}/instances/{instance}/databases/{database}/sessions/{session}",
            project=project,
            instance=instance,
            database=database,
            session=session,
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
            transport (Union[~.SpannerGrpcTransport,
                    Callable[[~.Credentials, type], ~.SpannerGrpcTransport]): A transport
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
            client_config = spanner_client_config.config

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
                    default_class=spanner_grpc_transport.SpannerGrpcTransport,
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
            self.transport = spanner_grpc_transport.SpannerGrpcTransport(
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
    def create_session(
        self,
        database,
        session=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        **Note:** This hint is currently ignored by PartitionQuery and
        PartitionRead requests.

        The desired maximum number of partitions to return. For example, this
        may be set to the number of workers available. The default for this
        option is currently 10,000. The maximum value is currently 200,000. This
        is only a hint. The actual number of partitions returned may be smaller
        or larger than this maximum count request.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> database = client.database_path('[PROJECT]', '[INSTANCE]', '[DATABASE]')
            >>>
            >>> response = client.create_session(database)

        Args:
            database (str): Required. The database in which the new session is created.
            session (Union[dict, ~google.cloud.spanner_v1.types.Session]): The session to create.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Session`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.Session` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "create_session" not in self._inner_api_calls:
            self._inner_api_calls[
                "create_session"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.create_session,
                default_retry=self._method_configs["CreateSession"].retry,
                default_timeout=self._method_configs["CreateSession"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.CreateSessionRequest(database=database, session=session)
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

        return self._inner_api_calls["create_session"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def batch_create_sessions(
        self,
        database,
        session_count,
        session_template=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Creates multiple new sessions.

        This API can be used to initialize a session cache on the clients.
        See https://goo.gl/TgSFN2 for best practices on session cache management.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> database = client.database_path('[PROJECT]', '[INSTANCE]', '[DATABASE]')
            >>>
            >>> # TODO: Initialize `session_count`:
            >>> session_count = 0
            >>>
            >>> response = client.batch_create_sessions(database, session_count)

        Args:
            database (str): Required. The database in which the new sessions are created.
            session_count (int): Set true to use the old proto1 MessageSet wire format for
                extensions. This is provided for backwards-compatibility with the
                MessageSet wire format. You should not use this for any other reason:
                It's less efficient, has fewer features, and is more complicated.

                The message must be defined exactly as follows: message Foo { option
                message_set_wire_format = true; extensions 4 to max; } Note that the
                message cannot have any defined fields; MessageSets only have
                extensions.

                All extensions of your type must be singular messages; e.g. they cannot
                be int32s, enums, or repeated messages.

                Because this is an option, the above two restrictions are not enforced
                by the protocol compiler.
            session_template (Union[dict, ~google.cloud.spanner_v1.types.Session]): Parameters to be applied to each created session.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Session`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.BatchCreateSessionsResponse` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "batch_create_sessions" not in self._inner_api_calls:
            self._inner_api_calls[
                "batch_create_sessions"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.batch_create_sessions,
                default_retry=self._method_configs["BatchCreateSessions"].retry,
                default_timeout=self._method_configs["BatchCreateSessions"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.BatchCreateSessionsRequest(
            database=database,
            session_count=session_count,
            session_template=session_template,
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

        return self._inner_api_calls["batch_create_sessions"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def get_session(
        self,
        name,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        ``TypeCode`` is used as part of ``Type`` to indicate the type of a
        Cloud Spanner value.

        Each legal value of a type can be encoded to or decoded from a JSON
        value, using the encodings described below. All Cloud Spanner values can
        be ``null``, regardless of type; ``null``\ s are always encoded as a
        JSON ``null``.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> name = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> response = client.get_session(name)

        Args:
            name (str): Required. The name of the session to retrieve.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.Session` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "get_session" not in self._inner_api_calls:
            self._inner_api_calls[
                "get_session"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.get_session,
                default_retry=self._method_configs["GetSession"].retry,
                default_timeout=self._method_configs["GetSession"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.GetSessionRequest(name=name)
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

        return self._inner_api_calls["get_session"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def list_sessions(
        self,
        database,
        page_size=None,
        filter_=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Lists all sessions in a given database.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> database = client.database_path('[PROJECT]', '[INSTANCE]', '[DATABASE]')
            >>>
            >>> # Iterate over all results
            >>> for element in client.list_sessions(database):
            ...     # process element
            ...     pass
            >>>
            >>>
            >>> # Alternatively:
            >>>
            >>> # Iterate over results one page at a time
            >>> for page in client.list_sessions(database).pages:
            ...     for element in page:
            ...         # process element
            ...         pass

        Args:
            database (str): Required. The database in which to list sessions.
            page_size (int): The maximum number of resources contained in the
                underlying API response. If page streaming is performed per-
                resource, this parameter does not affect the return value. If page
                streaming is performed per-page, this determines the maximum number
                of resources in a page.
            filter_ (str): Encoded as a base64-encoded ``string``, as described in RFC 4648,
                section 4.
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
            An iterable of :class:`~google.cloud.spanner_v1.types.Session` instances.
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
        if "list_sessions" not in self._inner_api_calls:
            self._inner_api_calls[
                "list_sessions"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.list_sessions,
                default_retry=self._method_configs["ListSessions"].retry,
                default_timeout=self._method_configs["ListSessions"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.ListSessionsRequest(
            database=database, page_size=page_size, filter=filter_
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

        iterator = google.api_core.page_iterator.GRPCIterator(
            client=None,
            method=functools.partial(
                self._inner_api_calls["list_sessions"],
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            ),
            request=request,
            items_field="sessions",
            request_token_field="page_token",
            response_token_field="next_page_token",
        )
        return iterator

    def delete_session(
        self,
        name,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Ends a session, releasing server resources associated with it. This will
        asynchronously trigger cancellation of any operations that are running with
        this session.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> name = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> client.delete_session(name)

        Args:
            name (str): Required. The name of the session to delete.
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
        if "delete_session" not in self._inner_api_calls:
            self._inner_api_calls[
                "delete_session"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.delete_session,
                default_retry=self._method_configs["DeleteSession"].retry,
                default_timeout=self._method_configs["DeleteSession"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.DeleteSessionRequest(name=name)
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

        self._inner_api_calls["delete_session"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def execute_sql(
        self,
        session,
        sql,
        transaction=None,
        params=None,
        param_types=None,
        resume_token=None,
        query_mode=None,
        partition_token=None,
        seqno=None,
        query_options=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        The request for ``PartitionRead``

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `sql`:
            >>> sql = ''
            >>>
            >>> response = client.execute_sql(session, sql)

        Args:
            session (str): Required. The session in which the SQL query should be performed.
            sql (str): Required. The SQL string.
            transaction (Union[dict, ~google.cloud.spanner_v1.types.TransactionSelector]): The transaction to use.

                For queries, if none is provided, the default is a temporary read-only
                transaction with strong concurrency.

                Standard DML statements require a read-write transaction. To protect
                against replays, single-use transactions are not supported.  The caller
                must either supply an existing transaction ID or begin a new transaction.

                Partitioned DML requires an existing Partitioned DML transaction ID.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionSelector`
            params (Union[dict, ~google.cloud.spanner_v1.types.Struct]): Encoded as ``list``, where list element ``i`` is represented
                according to
                [struct_type.fields[i]][google.spanner.v1.StructType.fields].

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Struct`
            param_types (dict[str -> Union[dict, ~google.cloud.spanner_v1.types.Type]]): Only present if the child node is ``SCALAR`` and corresponds to an
                output variable of the parent node. The field carries the name of the
                output variable. For example, a ``TableScan`` operator that reads rows
                from a table will have child links to the ``SCALAR`` nodes representing
                the output variables created for each column that is read by the
                operator. The corresponding ``variable`` fields will be set to the
                variable names assigned to the columns.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Type`
            resume_token (bytes): ``Type`` indicates the type of a Cloud Spanner value, as might be
                stored in a table cell or returned from an SQL query.
            query_mode (~google.cloud.spanner_v1.types.QueryMode): Optional. The historical or future-looking state of the resource
                pattern.

                Example:

                ::

                    // The InspectTemplate message originally only supported resource
                    // names with organization, and project was added later.
                    message InspectTemplate {
                      option (google.api.resource) = {
                        type: "dlp.googleapis.com/InspectTemplate"
                        pattern:
                        "organizations/{organization}/inspectTemplates/{inspect_template}"
                        pattern: "projects/{project}/inspectTemplates/{inspect_template}"
                        history: ORIGINALLY_SINGLE_PATTERN
                      };
                    }
            partition_token (bytes): Required. The ``TypeCode`` for this type.
            seqno (long): A per-transaction sequence number used to identify this request. This field
                makes each request idempotent such that if the request is received multiple
                times, at most one will succeed.

                The sequence number must be monotonically increasing within the
                transaction. If a request arrives for the first time with an out-of-order
                sequence number, the transaction may be aborted. Replays of previously
                handled requests will yield the same response as the first execution.

                Required for DML statements. Ignored for queries.
            query_options (Union[dict, ~google.cloud.spanner_v1.types.QueryOptions]): Query optimizer configuration to use for the given query.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.QueryOptions`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.ResultSet` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "execute_sql" not in self._inner_api_calls:
            self._inner_api_calls[
                "execute_sql"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.execute_sql,
                default_retry=self._method_configs["ExecuteSql"].retry,
                default_timeout=self._method_configs["ExecuteSql"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.ExecuteSqlRequest(
            session=session,
            sql=sql,
            transaction=transaction,
            params=params,
            param_types=param_types,
            resume_token=resume_token,
            query_mode=query_mode,
            partition_token=partition_token,
            seqno=seqno,
            query_options=query_options,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["execute_sql"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def execute_streaming_sql(
        self,
        session,
        sql,
        transaction=None,
        params=None,
        param_types=None,
        resume_token=None,
        query_mode=None,
        partition_token=None,
        seqno=None,
        query_options=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        An annotation that describes a resource definition, see
        ``ResourceDescriptor``.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `sql`:
            >>> sql = ''
            >>>
            >>> for element in client.execute_streaming_sql(session, sql):
            ...     # process element
            ...     pass

        Args:
            session (str): Required. The session in which the SQL query should be performed.
            sql (str): Required. The SQL string.
            transaction (Union[dict, ~google.cloud.spanner_v1.types.TransactionSelector]): The transaction to use.

                For queries, if none is provided, the default is a temporary read-only
                transaction with strong concurrency.

                Standard DML statements require a read-write transaction. To protect
                against replays, single-use transactions are not supported.  The caller
                must either supply an existing transaction ID or begin a new transaction.

                Partitioned DML requires an existing Partitioned DML transaction ID.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionSelector`
            params (Union[dict, ~google.cloud.spanner_v1.types.Struct]): Encoded as ``list``, where list element ``i`` is represented
                according to
                [struct_type.fields[i]][google.spanner.v1.StructType.fields].

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Struct`
            param_types (dict[str -> Union[dict, ~google.cloud.spanner_v1.types.Type]]): Only present if the child node is ``SCALAR`` and corresponds to an
                output variable of the parent node. The field carries the name of the
                output variable. For example, a ``TableScan`` operator that reads rows
                from a table will have child links to the ``SCALAR`` nodes representing
                the output variables created for each column that is read by the
                operator. The corresponding ``variable`` fields will be set to the
                variable names assigned to the columns.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Type`
            resume_token (bytes): ``Type`` indicates the type of a Cloud Spanner value, as might be
                stored in a table cell or returned from an SQL query.
            query_mode (~google.cloud.spanner_v1.types.QueryMode): Optional. The historical or future-looking state of the resource
                pattern.

                Example:

                ::

                    // The InspectTemplate message originally only supported resource
                    // names with organization, and project was added later.
                    message InspectTemplate {
                      option (google.api.resource) = {
                        type: "dlp.googleapis.com/InspectTemplate"
                        pattern:
                        "organizations/{organization}/inspectTemplates/{inspect_template}"
                        pattern: "projects/{project}/inspectTemplates/{inspect_template}"
                        history: ORIGINALLY_SINGLE_PATTERN
                      };
                    }
            partition_token (bytes): Required. The ``TypeCode`` for this type.
            seqno (long): A per-transaction sequence number used to identify this request. This field
                makes each request idempotent such that if the request is received multiple
                times, at most one will succeed.

                The sequence number must be monotonically increasing within the
                transaction. If a request arrives for the first time with an out-of-order
                sequence number, the transaction may be aborted. Replays of previously
                handled requests will yield the same response as the first execution.

                Required for DML statements. Ignored for queries.
            query_options (Union[dict, ~google.cloud.spanner_v1.types.QueryOptions]): Query optimizer configuration to use for the given query.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.QueryOptions`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            Iterable[~google.cloud.spanner_v1.types.PartialResultSet].

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "execute_streaming_sql" not in self._inner_api_calls:
            self._inner_api_calls[
                "execute_streaming_sql"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.execute_streaming_sql,
                default_retry=self._method_configs["ExecuteStreamingSql"].retry,
                default_timeout=self._method_configs["ExecuteStreamingSql"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.ExecuteSqlRequest(
            session=session,
            sql=sql,
            transaction=transaction,
            params=params,
            param_types=param_types,
            resume_token=resume_token,
            query_mode=query_mode,
            partition_token=partition_token,
            seqno=seqno,
            query_options=query_options,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["execute_streaming_sql"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def execute_batch_dml(
        self,
        session,
        transaction,
        statements,
        seqno,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Encoded as JSON ``true`` or ``false``.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `transaction`:
            >>> transaction = {}
            >>>
            >>> # TODO: Initialize `statements`:
            >>> statements = []
            >>>
            >>> # TODO: Initialize `seqno`:
            >>> seqno = 0
            >>>
            >>> response = client.execute_batch_dml(session, transaction, statements, seqno)

        Args:
            session (str): Required. The session in which the DML statements should be performed.
            transaction (Union[dict, ~google.cloud.spanner_v1.types.TransactionSelector]): Required. The transaction to use. Must be a read-write transaction.

                To protect against replays, single-use transactions are not supported. The
                caller must either supply an existing transaction ID or begin a new
                transaction.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionSelector`
            statements (list[Union[dict, ~google.cloud.spanner_v1.types.Statement]]): If ``code`` == ``ARRAY``, then ``array_element_type`` is the type of
                the array elements.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Statement`
            seqno (long): Required. A per-transaction sequence number used to identify this request. This field
                makes each request idempotent such that if the request is received multiple
                times, at most one will succeed.

                The sequence number must be monotonically increasing within the
                transaction. If a request arrives for the first time with an out-of-order
                sequence number, the transaction may be aborted. Replays of previously
                handled requests will yield the same response as the first execution.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.ExecuteBatchDmlResponse` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "execute_batch_dml" not in self._inner_api_calls:
            self._inner_api_calls[
                "execute_batch_dml"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.execute_batch_dml,
                default_retry=self._method_configs["ExecuteBatchDml"].retry,
                default_timeout=self._method_configs["ExecuteBatchDml"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.ExecuteBatchDmlRequest(
            session=session, transaction=transaction, statements=statements, seqno=seqno
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["execute_batch_dml"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def read(
        self,
        session,
        table,
        columns,
        key_set,
        transaction=None,
        index=None,
        limit=None,
        resume_token=None,
        partition_token=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        A simple descriptor of a resource type.

        ResourceDescriptor annotates a resource message (either by means of a
        protobuf annotation or use in the service config), and associates the
        resource's schema, the resource type, and the pattern of the resource
        name.

        Example:

        ::

            message Topic {
              // Indicates this message defines a resource schema.
              // Declares the resource type in the format of {service}/{kind}.
              // For Kubernetes resources, the format is {api group}/{kind}.
              option (google.api.resource) = {
                type: "pubsub.googleapis.com/Topic"
                name_descriptor: {
                  pattern: "projects/{project}/topics/{topic}"
                  parent_type: "cloudresourcemanager.googleapis.com/Project"
                  parent_name_extractor: "projects/{project}"
                }
              };
            }

        The ResourceDescriptor Yaml config will look like:

        ::

            resources:
            - type: "pubsub.googleapis.com/Topic"
              name_descriptor:
                - pattern: "projects/{project}/topics/{topic}"
                  parent_type: "cloudresourcemanager.googleapis.com/Project"
                  parent_name_extractor: "projects/{project}"

        Sometimes, resources have multiple patterns, typically because they can
        live under multiple parents.

        Example:

        ::

            message LogEntry {
              option (google.api.resource) = {
                type: "logging.googleapis.com/LogEntry"
                name_descriptor: {
                  pattern: "projects/{project}/logs/{log}"
                  parent_type: "cloudresourcemanager.googleapis.com/Project"
                  parent_name_extractor: "projects/{project}"
                }
                name_descriptor: {
                  pattern: "folders/{folder}/logs/{log}"
                  parent_type: "cloudresourcemanager.googleapis.com/Folder"
                  parent_name_extractor: "folders/{folder}"
                }
                name_descriptor: {
                  pattern: "organizations/{organization}/logs/{log}"
                  parent_type: "cloudresourcemanager.googleapis.com/Organization"
                  parent_name_extractor: "organizations/{organization}"
                }
                name_descriptor: {
                  pattern: "billingAccounts/{billing_account}/logs/{log}"
                  parent_type: "billing.googleapis.com/BillingAccount"
                  parent_name_extractor: "billingAccounts/{billing_account}"
                }
              };
            }

        The ResourceDescriptor Yaml config will look like:

        ::

            resources:
            - type: 'logging.googleapis.com/LogEntry'
              name_descriptor:
                - pattern: "projects/{project}/logs/{log}"
                  parent_type: "cloudresourcemanager.googleapis.com/Project"
                  parent_name_extractor: "projects/{project}"
                - pattern: "folders/{folder}/logs/{log}"
                  parent_type: "cloudresourcemanager.googleapis.com/Folder"
                  parent_name_extractor: "folders/{folder}"
                - pattern: "organizations/{organization}/logs/{log}"
                  parent_type: "cloudresourcemanager.googleapis.com/Organization"
                  parent_name_extractor: "organizations/{organization}"
                - pattern: "billingAccounts/{billing_account}/logs/{log}"
                  parent_type: "billing.googleapis.com/BillingAccount"
                  parent_name_extractor: "billingAccounts/{billing_account}"

        For flexible resources, the resource name doesn't contain parent names,
        but the resource itself has parents for policy evaluation.

        Example:

        ::

            message Shelf {
              option (google.api.resource) = {
                type: "library.googleapis.com/Shelf"
                name_descriptor: {
                  pattern: "shelves/{shelf}"
                  parent_type: "cloudresourcemanager.googleapis.com/Project"
                }
                name_descriptor: {
                  pattern: "shelves/{shelf}"
                  parent_type: "cloudresourcemanager.googleapis.com/Folder"
                }
              };
            }

        The ResourceDescriptor Yaml config will look like:

        ::

            resources:
            - type: 'library.googleapis.com/Shelf'
              name_descriptor:
                - pattern: "shelves/{shelf}"
                  parent_type: "cloudresourcemanager.googleapis.com/Project"
                - pattern: "shelves/{shelf}"
                  parent_type: "cloudresourcemanager.googleapis.com/Folder"

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `table`:
            >>> table = ''
            >>>
            >>> # TODO: Initialize `columns`:
            >>> columns = []
            >>>
            >>> # TODO: Initialize `key_set`:
            >>> key_set = {}
            >>>
            >>> response = client.read(session, table, columns, key_set)

        Args:
            session (str): Required. The session in which the read should be performed.
            table (str): Required. The name of the table in the database to be read.
            columns (list[str]): The list of fields that make up this struct. Order is significant,
                because values of this struct type are represented as lists, where the
                order of field values matches the order of fields in the ``StructType``.
                In turn, the order of fields matches the order of columns in a read
                request, or the order of fields in the ``SELECT`` clause of a query.
            key_set (Union[dict, ~google.cloud.spanner_v1.types.KeySet]): Used to determine the type of node. May be needed for visualizing
                different kinds of nodes differently. For example, If the node is a
                ``SCALAR`` node, it will have a condensed representation which can be
                used to directly embed a description of the node in its parent.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.KeySet`
            transaction (Union[dict, ~google.cloud.spanner_v1.types.TransactionSelector]): The transaction to use. If none is provided, the default is a
                temporary read-only transaction with strong concurrency.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionSelector`
            index (str): Protocol Buffers - Google's data interchange format Copyright 2008
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
            limit (long): Required. ``key_set`` identifies the rows to be yielded. ``key_set``
                names the primary keys of the rows in ``table`` to be yielded, unless
                ``index`` is present. If ``index`` is present, then ``key_set`` instead
                names index keys in ``index``.

                It is not an error for the ``key_set`` to name rows that do not exist in
                the database. Read yields nothing for nonexistent rows.
            resume_token (bytes): If this SourceCodeInfo represents a complete declaration, these are
                any comments appearing before and after the declaration which appear to
                be attached to the declaration.

                A series of line comments appearing on consecutive lines, with no other
                tokens appearing on those lines, will be treated as a single comment.

                leading_detached_comments will keep paragraphs of comments that appear
                before (but not connected to) the current element. Each paragraph,
                separated by empty lines, will be one comment element in the repeated
                field.

                Only the comment content is provided; comment markers (e.g. //) are
                stripped out. For block comments, leading whitespace and an asterisk
                will be stripped from the beginning of each line other than the first.
                Newlines are included in the output.

                Examples:

                optional int32 foo = 1; // Comment attached to foo. // Comment attached
                to bar. optional int32 bar = 2;

                optional string baz = 3; // Comment attached to baz. // Another line
                attached to baz.

                // Comment attached to qux. // // Another line attached to qux. optional
                double qux = 4;

                // Detached comment for corge. This is not leading or trailing comments
                // to qux or corge because there are blank lines separating it from //
                both.

                // Detached comment for corge paragraph 2.

                optional string corge = 5; /\* Block comment attached \* to corge.
                Leading asterisks \* will be removed. */ /* Block comment attached to \*
                grault. \*/ optional int32 grault = 6;

                // ignored detached comments.
            partition_token (bytes): ``StructType`` defines the fields of a ``STRUCT`` type.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.ResultSet` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "read" not in self._inner_api_calls:
            self._inner_api_calls["read"] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.read,
                default_retry=self._method_configs["Read"].retry,
                default_timeout=self._method_configs["Read"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.ReadRequest(
            session=session,
            table=table,
            columns=columns,
            key_set=key_set,
            transaction=transaction,
            index=index,
            limit=limit,
            resume_token=resume_token,
            partition_token=partition_token,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["read"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def streaming_read(
        self,
        session,
        table,
        columns,
        key_set,
        transaction=None,
        index=None,
        limit=None,
        resume_token=None,
        partition_token=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Encoded as ``string``, in decimal format.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `table`:
            >>> table = ''
            >>>
            >>> # TODO: Initialize `columns`:
            >>> columns = []
            >>>
            >>> # TODO: Initialize `key_set`:
            >>> key_set = {}
            >>>
            >>> for element in client.streaming_read(session, table, columns, key_set):
            ...     # process element
            ...     pass

        Args:
            session (str): Required. The session in which the read should be performed.
            table (str): Required. The name of the table in the database to be read.
            columns (list[str]): The list of fields that make up this struct. Order is significant,
                because values of this struct type are represented as lists, where the
                order of field values matches the order of fields in the ``StructType``.
                In turn, the order of fields matches the order of columns in a read
                request, or the order of fields in the ``SELECT`` clause of a query.
            key_set (Union[dict, ~google.cloud.spanner_v1.types.KeySet]): Used to determine the type of node. May be needed for visualizing
                different kinds of nodes differently. For example, If the node is a
                ``SCALAR`` node, it will have a condensed representation which can be
                used to directly embed a description of the node in its parent.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.KeySet`
            transaction (Union[dict, ~google.cloud.spanner_v1.types.TransactionSelector]): The transaction to use. If none is provided, the default is a
                temporary read-only transaction with strong concurrency.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionSelector`
            index (str): Protocol Buffers - Google's data interchange format Copyright 2008
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
            limit (long): Required. ``key_set`` identifies the rows to be yielded. ``key_set``
                names the primary keys of the rows in ``table`` to be yielded, unless
                ``index`` is present. If ``index`` is present, then ``key_set`` instead
                names index keys in ``index``.

                It is not an error for the ``key_set`` to name rows that do not exist in
                the database. Read yields nothing for nonexistent rows.
            resume_token (bytes): If this SourceCodeInfo represents a complete declaration, these are
                any comments appearing before and after the declaration which appear to
                be attached to the declaration.

                A series of line comments appearing on consecutive lines, with no other
                tokens appearing on those lines, will be treated as a single comment.

                leading_detached_comments will keep paragraphs of comments that appear
                before (but not connected to) the current element. Each paragraph,
                separated by empty lines, will be one comment element in the repeated
                field.

                Only the comment content is provided; comment markers (e.g. //) are
                stripped out. For block comments, leading whitespace and an asterisk
                will be stripped from the beginning of each line other than the first.
                Newlines are included in the output.

                Examples:

                optional int32 foo = 1; // Comment attached to foo. // Comment attached
                to bar. optional int32 bar = 2;

                optional string baz = 3; // Comment attached to baz. // Another line
                attached to baz.

                // Comment attached to qux. // // Another line attached to qux. optional
                double qux = 4;

                // Detached comment for corge. This is not leading or trailing comments
                // to qux or corge because there are blank lines separating it from //
                both.

                // Detached comment for corge paragraph 2.

                optional string corge = 5; /\* Block comment attached \* to corge.
                Leading asterisks \* will be removed. */ /* Block comment attached to \*
                grault. \*/ optional int32 grault = 6;

                // ignored detached comments.
            partition_token (bytes): ``StructType`` defines the fields of a ``STRUCT`` type.
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            Iterable[~google.cloud.spanner_v1.types.PartialResultSet].

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "streaming_read" not in self._inner_api_calls:
            self._inner_api_calls[
                "streaming_read"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.streaming_read,
                default_retry=self._method_configs["StreamingRead"].retry,
                default_timeout=self._method_configs["StreamingRead"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.ReadRequest(
            session=session,
            table=table,
            columns=columns,
            key_set=key_set,
            transaction=transaction,
            index=index,
            limit=limit,
            resume_token=resume_token,
            partition_token=partition_token,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["streaming_read"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def begin_transaction(
        self,
        session,
        options_,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        The resource type. It must be in the format of
        {service_name}/{resource_type_kind}. The ``resource_type_kind`` must be
        singular and must not include version numbers.

        Example: ``storage.googleapis.com/Bucket``

        The value of the resource_type_kind must follow the regular expression
        /[A-Za-z][a-zA-Z0-9]+/. It should start with an upper case character and
        should use PascalCase (UpperCamelCase). The maximum number of characters
        allowed for the ``resource_type_kind`` is 100.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `options_`:
            >>> options_ = {}
            >>>
            >>> response = client.begin_transaction(session, options_)

        Args:
            session (str): Required. The session in which the transaction runs.
            options_ (Union[dict, ~google.cloud.spanner_v1.types.TransactionOptions]): Required. Options for the new transaction.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionOptions`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.Transaction` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "begin_transaction" not in self._inner_api_calls:
            self._inner_api_calls[
                "begin_transaction"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.begin_transaction,
                default_retry=self._method_configs["BeginTransaction"].retry,
                default_timeout=self._method_configs["BeginTransaction"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.BeginTransactionRequest(session=session, options=options_)
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["begin_transaction"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def commit(
        self,
        session,
        transaction_id=None,
        single_use_transaction=None,
        mutations=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Creates a set of partition tokens that can be used to execute a
        query operation in parallel. Each of the returned partition tokens can
        be used by ``ExecuteStreamingSql`` to specify a subset of the query
        result to read. The same session and read-only transaction must be used
        by the PartitionQueryRequest used to create the partition tokens and the
        ExecuteSqlRequests that use the partition tokens.

        Partition tokens become invalid when the session used to create them is
        deleted, is idle for too long, begins a new transaction, or becomes too
        old. When any of these happen, it is not possible to resume the query,
        and the whole operation must be restarted from the beginning.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> response = client.commit(session)

        Args:
            session (str): Required. The session in which the transaction to be committed is running.
            transaction_id (bytes): Commit a previously-started transaction.
            single_use_transaction (Union[dict, ~google.cloud.spanner_v1.types.TransactionOptions]): Streaming calls might be interrupted for a variety of reasons, such
                as TCP connection loss. If this occurs, the stream of results can be
                resumed by re-sending the original request and including
                ``resume_token``. Note that executing any other transaction in the same
                session invalidates the token.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionOptions`
            mutations (list[Union[dict, ~google.cloud.spanner_v1.types.Mutation]]): The mutations to be executed when this transaction commits. All
                mutations are applied atomically, in the order they appear in
                this list.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Mutation`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.CommitResponse` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "commit" not in self._inner_api_calls:
            self._inner_api_calls[
                "commit"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.commit,
                default_retry=self._method_configs["Commit"].retry,
                default_timeout=self._method_configs["Commit"].timeout,
                client_info=self._client_info,
            )

        # Sanity check: We have some fields which are mutually exclusive;
        # raise ValueError if more than one is sent.
        google.api_core.protobuf_helpers.check_oneof(
            transaction_id=transaction_id, single_use_transaction=single_use_transaction
        )

        request = spanner_pb2.CommitRequest(
            session=session,
            transaction_id=transaction_id,
            single_use_transaction=single_use_transaction,
            mutations=mutations,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["commit"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def rollback(
        self,
        session,
        transaction_id,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Encoded as ``number``, or the strings ``"NaN"``, ``"Infinity"``, or
        ``"-Infinity"``.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `transaction_id`:
            >>> transaction_id = b''
            >>>
            >>> client.rollback(session, transaction_id)

        Args:
            session (str): Required. The session in which the transaction to roll back is running.
            transaction_id (bytes): Required. The transaction to roll back.
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
        if "rollback" not in self._inner_api_calls:
            self._inner_api_calls[
                "rollback"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.rollback,
                default_retry=self._method_configs["Rollback"].retry,
                default_timeout=self._method_configs["Rollback"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.RollbackRequest(
            session=session, transaction_id=transaction_id
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        self._inner_api_calls["rollback"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def partition_query(
        self,
        session,
        sql,
        transaction=None,
        params=None,
        param_types=None,
        partition_options=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Encoded as ``string`` in RFC 3339 timestamp format. The time zone
        must be present, and must be ``"Z"``.

        If the schema has the column option ``allow_commit_timestamp=true``, the
        placeholder string ``"spanner.commit_timestamp()"`` can be used to
        instruct the system to insert the commit timestamp associated with the
        transaction commit.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `sql`:
            >>> sql = ''
            >>>
            >>> response = client.partition_query(session, sql)

        Args:
            session (str): Required. The session used to create the partitions.
            sql (str): The resource type that the annotated field references.

                Example:

                ::

                    message Subscription {
                      string topic = 2 [(google.api.resource_reference) = {
                        type: "pubsub.googleapis.com/Topic"
                      }];
                    }
            transaction (Union[dict, ~google.cloud.spanner_v1.types.TransactionSelector]): Read only snapshot transactions are supported, read/write and single use
                transactions are not.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionSelector`
            params (Union[dict, ~google.cloud.spanner_v1.types.Struct]): Required. The number of sessions to be created in this batch call.
                The API may return fewer than the requested number of sessions. If a
                specific number of sessions are desired, the client can make additional
                calls to BatchCreateSessions (adjusting ``session_count`` as necessary).

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Struct`
            param_types (dict[str -> Union[dict, ~google.cloud.spanner_v1.types.Type]]): The request for ``Rollback``.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.Type`
            partition_options (Union[dict, ~google.cloud.spanner_v1.types.PartitionOptions]): Additional options that affect how many partitions are created.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.PartitionOptions`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.PartitionResponse` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "partition_query" not in self._inner_api_calls:
            self._inner_api_calls[
                "partition_query"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.partition_query,
                default_retry=self._method_configs["PartitionQuery"].retry,
                default_timeout=self._method_configs["PartitionQuery"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.PartitionQueryRequest(
            session=session,
            sql=sql,
            transaction=transaction,
            params=params,
            param_types=param_types,
            partition_options=partition_options,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["partition_query"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )

    def partition_read(
        self,
        session,
        table,
        key_set,
        transaction=None,
        index=None,
        columns=None,
        partition_options=None,
        retry=google.api_core.gapic_v1.method.DEFAULT,
        timeout=google.api_core.gapic_v1.method.DEFAULT,
        metadata=None,
    ):
        """
        Required. The query request to generate partitions for. The request
        will fail if the query is not root partitionable. The query plan of a
        root partitionable query has a single distributed union operator. A
        distributed union operator conceptually divides one or more tables into
        multiple splits, remotely evaluates a subquery independently on each
        split, and then unions all results.

        This must not contain DML commands, such as INSERT, UPDATE, or DELETE.
        Use ``ExecuteStreamingSql`` with a PartitionedDml transaction for large,
        partition-friendly DML operations.

        Example:
            >>> from google.cloud import spanner_v1
            >>>
            >>> client = spanner_v1.SpannerClient()
            >>>
            >>> session = client.session_path('[PROJECT]', '[INSTANCE]', '[DATABASE]', '[SESSION]')
            >>>
            >>> # TODO: Initialize `table`:
            >>> table = ''
            >>>
            >>> # TODO: Initialize `key_set`:
            >>> key_set = {}
            >>>
            >>> response = client.partition_read(session, table, key_set)

        Args:
            session (str): Required. The session used to create the partitions.
            table (str): Required. The name of the table in the database to be read.
            key_set (Union[dict, ~google.cloud.spanner_v1.types.KeySet]): Identifies which part of the FileDescriptorProto was defined at this
                location.

                Each element is a field number or an index. They form a path from the
                root FileDescriptorProto to the place where the definition. For example,
                this path: [ 4, 3, 2, 7, 1 ] refers to: file.message_type(3) // 4, 3
                .field(7) // 2, 7 .name() // 1 This is because
                FileDescriptorProto.message_type has field number 4: repeated
                DescriptorProto message_type = 4; and DescriptorProto.field has field
                number 2: repeated FieldDescriptorProto field = 2; and
                FieldDescriptorProto.name has field number 1: optional string name = 1;

                Thus, the above path gives the location of a field name. If we removed
                the last element: [ 4, 3, 2, 7 ] this path refers to the whole field
                declaration (from the beginning of the label to the terminating
                semicolon).

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.KeySet`
            transaction (Union[dict, ~google.cloud.spanner_v1.types.TransactionSelector]): Read only snapshot transactions are supported, read/write and single use
                transactions are not.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.TransactionSelector`
            index (str): Denotes a Relational operator node in the expression tree.
                Relational operators represent iterative processing of rows during query
                execution. For example, a ``TableScan`` operation that reads rows from a
                table.
            columns (list[str]): The resource type of a child collection that the annotated field
                references. This is useful for annotating the ``parent`` field that
                doesn't have a fixed resource type.

                Example:

                ::

                    message ListLogEntriesRequest {
                      string parent = 1 [(google.api.resource_reference) = {
                        child_type: "logging.googleapis.com/LogEntry"
                      };
                    }
            partition_options (Union[dict, ~google.cloud.spanner_v1.types.PartitionOptions]): Additional options that affect how many partitions are created.

                If a dict is provided, it must be of the same form as the protobuf
                message :class:`~google.cloud.spanner_v1.types.PartitionOptions`
            retry (Optional[google.api_core.retry.Retry]):  A retry object used
                to retry requests. If ``None`` is specified, requests will
                be retried using a default configuration.
            timeout (Optional[float]): The amount of time, in seconds, to wait
                for the request to complete. Note that if ``retry`` is
                specified, the timeout applies to each individual attempt.
            metadata (Optional[Sequence[Tuple[str, str]]]): Additional metadata
                that is provided to the method.

        Returns:
            A :class:`~google.cloud.spanner_v1.types.PartitionResponse` instance.

        Raises:
            google.api_core.exceptions.GoogleAPICallError: If the request
                    failed for any reason.
            google.api_core.exceptions.RetryError: If the request failed due
                    to a retryable error and retry attempts failed.
            ValueError: If the parameters are invalid.
        """
        # Wrap the transport method to add retry and timeout logic.
        if "partition_read" not in self._inner_api_calls:
            self._inner_api_calls[
                "partition_read"
            ] = google.api_core.gapic_v1.method.wrap_method(
                self.transport.partition_read,
                default_retry=self._method_configs["PartitionRead"].retry,
                default_timeout=self._method_configs["PartitionRead"].timeout,
                client_info=self._client_info,
            )

        request = spanner_pb2.PartitionReadRequest(
            session=session,
            table=table,
            key_set=key_set,
            transaction=transaction,
            index=index,
            columns=columns,
            partition_options=partition_options,
        )
        if metadata is None:
            metadata = []
        metadata = list(metadata)
        try:
            routing_header = [("session", session)]
        except AttributeError:
            pass
        else:
            routing_metadata = google.api_core.gapic_v1.routing_header.to_grpc_metadata(
                routing_header
            )
            metadata.append(routing_metadata)

        return self._inner_api_calls["partition_read"](
            request, retry=retry, timeout=timeout, metadata=metadata
        )
