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


import google.api_core.grpc_helpers
import google.api_core.operations_v1

from google.cloud.spanner_admin_database_v1.proto import spanner_database_admin_pb2_grpc


class DatabaseAdminGrpcTransport(object):
    """gRPC transport class providing stubs for
    google.spanner.admin.database.v1 DatabaseAdmin API.

    The transport provides access to the raw gRPC stubs,
    which can be used to take advantage of advanced
    features of gRPC.
    """

    # The scopes needed to make gRPC calls to all of the methods defined
    # in this service.
    _OAUTH_SCOPES = (
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/spanner.admin",
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
                }.items(),
            )

        self._channel = channel

        # gRPC uses objects called "stubs" that are bound to the
        # channel and provide a basic method for each RPC.
        self._stubs = {
            "database_admin_stub": spanner_database_admin_pb2_grpc.DatabaseAdminStub(
                channel
            )
        }

        # Because this API includes a method that returns a
        # long-running operation (proto: google.longrunning.Operation),
        # instantiate an LRO client.
        self._operations_client = google.api_core.operations_v1.OperationsClient(
            channel
        )

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
    def create_database(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.create_database`.

        Denotes a field as required. This indicates that the field **must**
        be provided as part of the request, and failure to do so will cause an
        error (usually ``INVALID_ARGUMENT``).

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].CreateDatabase

    @property
    def get_database(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.get_database`.

        Gets the state of a Cloud Spanner database.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].GetDatabase

    @property
    def update_database_ddl(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.update_database_ddl`.

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

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].UpdateDatabaseDdl

    @property
    def drop_database(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.drop_database`.

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

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].DropDatabase

    @property
    def get_database_ddl(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.get_database_ddl`.

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

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].GetDatabaseDdl

    @property
    def set_iam_policy(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.set_iam_policy`.

        Denotes a field as output only. This indicates that the field is
        provided in responses, but including the field in a request does nothing
        (the server *must* ignore it and *must not* throw an error as a result
        of the field's presence).

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].SetIamPolicy

    @property
    def get_iam_policy(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.get_iam_policy`.

        Request message for ``SetIamPolicy`` method.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].GetIamPolicy

    @property
    def test_iam_permissions(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.test_iam_permissions`.

        The database is fully created and ready for use, but is still being
        optimized for performance and cannot handle full load.

        In this state, the database still references the backup it was restore
        from, preventing the backup from being deleted. When optimizations are
        complete, the full performance of the database will be restored, and the
        database will transition to ``READY`` state.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].TestIamPermissions

    @property
    def create_backup(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.create_backup`.

        Required. The name of the database. Values are of the form
        ``projects/<project>/instances/<instance>/databases/<database>``, where
        ``<database>`` is as specified in the ``CREATE DATABASE`` statement.
        This name can be passed to other API methods to identify the database.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].CreateBackup

    @property
    def get_backup(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.get_backup`.

        If non-empty, ``page_token`` should contain a ``next_page_token``
        from a previous ``ListBackupOperationsResponse`` to the same ``parent``
        and with the same ``filter``.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].GetBackup

    @property
    def update_backup(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.update_backup`.

        Not ZigZag encoded. Negative numbers take 10 bytes. Use TYPE_SINT64
        if negative values are likely.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].UpdateBackup

    @property
    def delete_backup(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.delete_backup`.

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

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].DeleteBackup

    @property
    def list_backups(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.list_backups`.

        REQUIRED: The complete policy to be applied to the ``resource``. The
        size of the policy is limited to a few 10s of KB. An empty policy is a
        valid policy but certain Cloud Platform services (such as Projects)
        might reject them.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].ListBackups

    @property
    def restore_database(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.restore_database`.

        The response for ``ListBackupOperations``.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].RestoreDatabase

    @property
    def list_database_operations(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.list_database_operations`.

        The list of matching backup ``long-running operations``. Each
        operation's name will be prefixed by the backup's name and the
        operation's ``metadata`` will be of type ``CreateBackupMetadata``.
        Operations returned include those that are pending or have
        completed/failed/canceled within the last 7 days. Operations returned
        are ordered by ``operation.metadata.value.progress.start_time`` in
        descending order starting from the most recently started operation.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].ListDatabaseOperations

    @property
    def list_backup_operations(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.list_backup_operations`.

        Not ZigZag encoded. Negative numbers take 10 bytes. Use TYPE_SINT32
        if negative values are likely.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].ListBackupOperations

    @property
    def list_databases(self):
        """Return the gRPC stub for :meth:`DatabaseAdminClient.list_databases`.

        Lists Cloud Spanner databases.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["database_admin_stub"].ListDatabases
