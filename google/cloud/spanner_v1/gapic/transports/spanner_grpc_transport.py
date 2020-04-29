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


import pkg_resources
import grpc_gcp

import google.api_core.grpc_helpers

from google.cloud.spanner_v1.proto import spanner_pb2_grpc


_GRPC_KEEPALIVE_MS = 2 * 60 * 1000
_SPANNER_GRPC_CONFIG = "spanner.grpc.config"


class SpannerGrpcTransport(object):
    """gRPC transport class providing stubs for
    google.spanner.v1 Spanner API.

    The transport provides access to the raw gRPC stubs,
    which can be used to take advantage of advanced
    features of gRPC.
    """

    # The scopes needed to make gRPC calls to all of the methods defined
    # in this service.
    _OAUTH_SCOPES = (
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/spanner.data",
    )

    def __init__(
        self, channel=None, credentials=None, address="spanner.googleapis.com:443"
    ):
        """Instantiate the transport class.

        Args:
            channel (grpc.Channel): A ``Channel`` instance through
                which to make calls. This argument is mutually exclusive
                with ``credentials``; providing both will raise an exception.
            credentials (google.auth.credentials.Credentials): The
                authorization credentials to attach to requests. These
                credentials identify this application to the service. If none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            address (str): The address where the service is hosted.
        """
        # If both `channel` and `credentials` are specified, raise an
        # exception (channels come with credentials baked in already).
        if channel is not None and credentials is not None:
            raise ValueError(
                "The `channel` and `credentials` arguments are mutually " "exclusive."
            )

        # Create the channel.
        if channel is None:
            channel = self.create_channel(
                address=address,
                credentials=credentials,
                options={
                    "grpc.max_send_message_length": -1,
                    "grpc.max_receive_message_length": -1,
                    "grpc.keepalive_time_ms": _GRPC_KEEPALIVE_MS,
                }.items(),
            )

        self._channel = channel

        # gRPC uses objects called "stubs" that are bound to the
        # channel and provide a basic method for each RPC.
        self._stubs = {"spanner_stub": spanner_pb2_grpc.SpannerStub(channel)}

    @classmethod
    def create_channel(
        cls, address="spanner.googleapis.com:443", credentials=None, **kwargs
    ):
        """Create and return a gRPC channel object.

        Args:
            address (str): The host for the channel to use.
            credentials (~.Credentials): The
                authorization credentials to attach to requests. These
                credentials identify this application to the service. If
                none are specified, the client will attempt to ascertain
                the credentials from the environment.
            kwargs (dict): Keyword arguments, which are passed to the
                channel creation.

        Returns:
            grpc.Channel: A gRPC channel object.
        """
        grpc_gcp_config = grpc_gcp.api_config_from_text_pb(
            pkg_resources.resource_string(__name__, _SPANNER_GRPC_CONFIG)
        )
        options = [(grpc_gcp.API_CONFIG_CHANNEL_ARG, grpc_gcp_config)]
        if "options" in kwargs:
            options.extend(kwargs["options"])
        kwargs["options"] = options
        return google.api_core.grpc_helpers.create_channel(
            address, credentials=credentials, scopes=cls._OAUTH_SCOPES, **kwargs
        )

    @property
    def channel(self):
        """The gRPC channel used by the transport.

        Returns:
            grpc.Channel: A gRPC channel object.
        """
        return self._channel

    @property
    def create_session(self):
        """Return the gRPC stub for :meth:`SpannerClient.create_session`.

        **Note:** This hint is currently ignored by PartitionQuery and
        PartitionRead requests.

        The desired maximum number of partitions to return. For example, this
        may be set to the number of workers available. The default for this
        option is currently 10,000. The maximum value is currently 200,000. This
        is only a hint. The actual number of partitions returned may be smaller
        or larger than this maximum count request.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].CreateSession

    @property
    def batch_create_sessions(self):
        """Return the gRPC stub for :meth:`SpannerClient.batch_create_sessions`.

        Creates multiple new sessions.

        This API can be used to initialize a session cache on the clients.
        See https://goo.gl/TgSFN2 for best practices on session cache management.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].BatchCreateSessions

    @property
    def get_session(self):
        """Return the gRPC stub for :meth:`SpannerClient.get_session`.

        ``TypeCode`` is used as part of ``Type`` to indicate the type of a
        Cloud Spanner value.

        Each legal value of a type can be encoded to or decoded from a JSON
        value, using the encodings described below. All Cloud Spanner values can
        be ``null``, regardless of type; ``null``\ s are always encoded as a
        JSON ``null``.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].GetSession

    @property
    def list_sessions(self):
        """Return the gRPC stub for :meth:`SpannerClient.list_sessions`.

        Lists all sessions in a given database.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].ListSessions

    @property
    def delete_session(self):
        """Return the gRPC stub for :meth:`SpannerClient.delete_session`.

        Ends a session, releasing server resources associated with it. This will
        asynchronously trigger cancellation of any operations that are running with
        this session.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].DeleteSession

    @property
    def execute_sql(self):
        """Return the gRPC stub for :meth:`SpannerClient.execute_sql`.

        The request for ``PartitionRead``

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].ExecuteSql

    @property
    def execute_streaming_sql(self):
        """Return the gRPC stub for :meth:`SpannerClient.execute_streaming_sql`.

        An annotation that describes a resource definition, see
        ``ResourceDescriptor``.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].ExecuteStreamingSql

    @property
    def execute_batch_dml(self):
        """Return the gRPC stub for :meth:`SpannerClient.execute_batch_dml`.

        Encoded as JSON ``true`` or ``false``.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].ExecuteBatchDml

    @property
    def read(self):
        """Return the gRPC stub for :meth:`SpannerClient.read`.

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

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].Read

    @property
    def streaming_read(self):
        """Return the gRPC stub for :meth:`SpannerClient.streaming_read`.

        Encoded as ``string``, in decimal format.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].StreamingRead

    @property
    def begin_transaction(self):
        """Return the gRPC stub for :meth:`SpannerClient.begin_transaction`.

        The resource type. It must be in the format of
        {service_name}/{resource_type_kind}. The ``resource_type_kind`` must be
        singular and must not include version numbers.

        Example: ``storage.googleapis.com/Bucket``

        The value of the resource_type_kind must follow the regular expression
        /[A-Za-z][a-zA-Z0-9]+/. It should start with an upper case character and
        should use PascalCase (UpperCamelCase). The maximum number of characters
        allowed for the ``resource_type_kind`` is 100.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].BeginTransaction

    @property
    def commit(self):
        """Return the gRPC stub for :meth:`SpannerClient.commit`.

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

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].Commit

    @property
    def rollback(self):
        """Return the gRPC stub for :meth:`SpannerClient.rollback`.

        Encoded as ``number``, or the strings ``"NaN"``, ``"Infinity"``, or
        ``"-Infinity"``.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].Rollback

    @property
    def partition_query(self):
        """Return the gRPC stub for :meth:`SpannerClient.partition_query`.

        Encoded as ``string`` in RFC 3339 timestamp format. The time zone
        must be present, and must be ``"Z"``.

        If the schema has the column option ``allow_commit_timestamp=true``, the
        placeholder string ``"spanner.commit_timestamp()"`` can be used to
        instruct the system to insert the commit timestamp associated with the
        transaction commit.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].PartitionQuery

    @property
    def partition_read(self):
        """Return the gRPC stub for :meth:`SpannerClient.partition_read`.

        Required. The query request to generate partitions for. The request
        will fail if the query is not root partitionable. The query plan of a
        root partitionable query has a single distributed union operator. A
        distributed union operator conceptually divides one or more tables into
        multiple splits, remotely evaluates a subquery independently on each
        split, and then unions all results.

        This must not contain DML commands, such as INSERT, UPDATE, or DELETE.
        Use ``ExecuteStreamingSql`` with a PartitionedDml transaction for large,
        partition-friendly DML operations.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["spanner_stub"].PartitionRead
