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

from google.cloud.spanner_admin_instance_v1.proto import spanner_instance_admin_pb2_grpc


class InstanceAdminGrpcTransport(object):
    """gRPC transport class providing stubs for
    google.spanner.admin.instance.v1 InstanceAdmin API.

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
            "instance_admin_stub": spanner_instance_admin_pb2_grpc.InstanceAdminStub(
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
    def list_instance_configs(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.list_instance_configs`.

        Lists the supported instance configurations for a given project.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].ListInstanceConfigs

    @property
    def get_instance_config(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.get_instance_config`.

        Gets information about a particular instance configuration.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].GetInstanceConfig

    @property
    def list_instances(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.list_instances`.

        Lists all instances in the given project.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].ListInstances

    @property
    def get_instance(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.get_instance`.

        Gets information about a particular instance.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].GetInstance

    @property
    def create_instance(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.create_instance`.

        Signed seconds of the span of time. Must be from -315,576,000,000 to
        +315,576,000,000 inclusive. Note: these bounds are computed from: 60
        sec/min \* 60 min/hr \* 24 hr/day \* 365.25 days/year \* 10000 years

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].CreateInstance

    @property
    def update_instance(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.update_instance`.

        Input and output type names. These are resolved in the same way as
        FieldDescriptorProto.type_name, but must refer to a message type.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].UpdateInstance

    @property
    def delete_instance(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.delete_instance`.

        An annotation that describes a resource reference, see
        ``ResourceReference``.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].DeleteInstance

    @property
    def set_iam_policy(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.set_iam_policy`.

        If type_name is set, this need not be set. If both this and
        type_name are set, this must be one of TYPE_ENUM, TYPE_MESSAGE or
        TYPE_GROUP.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].SetIamPolicy

    @property
    def get_iam_policy(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.get_iam_policy`.

        Denotes a field as output only. This indicates that the field is
        provided in responses, but including the field in a request does nothing
        (the server *must* ignore it and *must not* throw an error as a result
        of the field's presence).

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].GetIamPolicy

    @property
    def test_iam_permissions(self):
        """Return the gRPC stub for :meth:`InstanceAdminClient.test_iam_permissions`.

        The server-assigned name, which is only unique within the same
        service that originally returns it. If you use the default HTTP mapping,
        the ``name`` should be a resource name ending with
        ``operations/{unique_id}``.

        Returns:
            Callable: A callable which accepts the appropriate
                deserialized request object and returns a
                deserialized response object.
        """
        return self._stubs["instance_admin_stub"].TestIamPermissions
