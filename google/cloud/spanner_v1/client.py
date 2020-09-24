# Copyright 2016 Google LLC All rights reserved.
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

"""Parent client for calling the Cloud Spanner API.

This is the base from which all interactions with the API occur.

In the hierarchy of API concepts

* a :class:`~google.cloud.spanner_v1.client.Client` owns an
  :class:`~google.cloud.spanner_v1.instance.Instance`
* a :class:`~google.cloud.spanner_v1.instance.Instance` owns a
  :class:`~google.cloud.spanner_v1.database.Database`
"""
import grpc
import os
import warnings

from google.api_core.gapic_v1 import client_info
from google.auth.credentials import AnonymousCredentials
import google.api_core.client_options

from google.cloud.spanner_admin_instance_v1.gapic.transports import (
    instance_admin_grpc_transport,
)

from google.cloud.spanner_admin_database_v1.gapic.transports import (
    database_admin_grpc_transport,
)

# pylint: disable=line-too-long
from google.cloud.spanner_admin_database_v1.gapic.database_admin_client import (  # noqa
    DatabaseAdminClient,
)
from google.cloud.spanner_admin_instance_v1.gapic.instance_admin_client import (  # noqa
    InstanceAdminClient,
)

# pylint: enable=line-too-long

from google.cloud.client import ClientWithProject
from google.cloud.spanner_v1 import __version__
from google.cloud.spanner_v1._helpers import _merge_query_options, _metadata_with_prefix
from google.cloud.spanner_v1.instance import DEFAULT_NODE_COUNT
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.proto.spanner_pb2 import ExecuteSqlRequest

_CLIENT_INFO = client_info.ClientInfo(client_library_version=__version__)
EMULATOR_ENV_VAR = "SPANNER_EMULATOR_HOST"
_EMULATOR_HOST_HTTP_SCHEME = (
    "%s contains a http scheme. When used with a scheme it may cause gRPC's "
    "DNS resolver to endlessly attempt to resolve. %s is intended to be used "
    "without a scheme: ex %s=localhost:8080."
) % ((EMULATOR_ENV_VAR,) * 3)
SPANNER_ADMIN_SCOPE = "https://www.googleapis.com/auth/spanner.admin"
OPTIMIZER_VERSION_ENV_VAR = "SPANNER_OPTIMIZER_VERSION"
_USER_AGENT_DEPRECATED = (
    "The 'user_agent' argument to 'Client' is deprecated / unused. "
    "Please pass an appropriate 'client_info' instead."
)


def _get_spanner_emulator_host():
    return os.getenv(EMULATOR_ENV_VAR)


def _get_spanner_optimizer_version():
    return os.getenv(OPTIMIZER_VERSION_ENV_VAR, "")


class InstanceConfig(object):
    """Named configurations for Spanner instances.

    :type name: str
    :param name: ID of the instance configuration

    :type display_name: str
    :param display_name: Name of the instance configuration
    """

    def __init__(self, name, display_name):
        self.name = name
        self.display_name = display_name

    @classmethod
    def from_pb(cls, config_pb):
        """Construct an instance from the equvalent protobuf.

        :type config_pb:
          :class:`~google.spanner.v1.spanner_instance_admin_pb2.InstanceConfig`
        :param config_pb: the protobuf to parse

        :rtype: :class:`InstanceConfig`
        :returns: an instance of this class
        """
        return cls(config_pb.name, config_pb.display_name)


class Client(ClientWithProject):
    """Client for interacting with Cloud Spanner API.

    .. note::

        Since the Cloud Spanner API requires the gRPC transport, no
        ``_http`` argument is accepted by this class.

    :type project: :class:`str` or :func:`unicode <unicode>`
    :param project: (Optional) The ID of the project which owns the
                    instances, tables and data. If not provided, will
                    attempt to determine from the environment.

    :type credentials:
        :class:`Credentials <google.auth.credentials.Credentials>` or
        :data:`NoneType <types.NoneType>`
    :param credentials: (Optional) The authorization credentials to attach to requests.
                        These credentials identify this application to the service.
                        If none are specified, the client will attempt to ascertain
                        the credentials from the environment.

    :type client_info: :class:`~google.api_core.gapic_v1.client_info.ClientInfo`
    :param client_info:
        (Optional) The client info used to send a user-agent string along with
        API requests. If ``None``, then default info will be used. Generally,
        you only need to set this if you're developing your own library or
        partner tool.

    :type user_agent: str
    :param user_agent:
        (Deprecated) The user agent to be used with API request.
        Not used.

    :type client_options: :class:`~google.api_core.client_options.ClientOptions`
        or :class:`dict`
    :param client_options: (Optional) Client options used to set user options
        on the client. API Endpoint should be set through client_options.

    :type query_options:
        :class:`~google.cloud.spanner_v1.proto.ExecuteSqlRequest.QueryOptions`
        or :class:`dict`
    :param query_options:
        (Optional) Query optimizer configuration to use for the given query.
        If a dict is provided, it must be of the same form as the protobuf
        message :class:`~google.cloud.spanner_v1.types.QueryOptions`

    :raises: :class:`ValueError <exceptions.ValueError>` if both ``read_only``
             and ``admin`` are :data:`True`
    """

    _instance_admin_api = None
    _database_admin_api = None
    user_agent = None
    _SET_PROJECT = True  # Used by from_service_account_json()

    SCOPE = (SPANNER_ADMIN_SCOPE,)
    """The scopes required for Google Cloud Spanner."""

    def __init__(
        self,
        project=None,
        credentials=None,
        client_info=_CLIENT_INFO,
        user_agent=None,
        client_options=None,
        query_options=None,
    ):
        self._emulator_host = _get_spanner_emulator_host()

        if client_options and type(client_options) == dict:
            self._client_options = google.api_core.client_options.from_dict(
                client_options
            )
        else:
            self._client_options = client_options

        if self._emulator_host:
            credentials = AnonymousCredentials()
        elif isinstance(credentials, AnonymousCredentials):
            self._emulator_host = self._client_options.api_endpoint

        # NOTE: This API has no use for the _http argument, but sending it
        #       will have no impact since the _http() @property only lazily
        #       creates a working HTTP object.
        super(Client, self).__init__(
            project=project,
            credentials=credentials,
            client_options=client_options,
            _http=None,
        )
        self._client_info = client_info

        env_query_options = ExecuteSqlRequest.QueryOptions(
            optimizer_version=_get_spanner_optimizer_version()
        )

        # Environment flag config has higher precedence than application config.
        self._query_options = _merge_query_options(query_options, env_query_options)

        if user_agent is not None:
            warnings.warn(_USER_AGENT_DEPRECATED, DeprecationWarning, stacklevel=2)
            self.user_agent = user_agent

        if self._emulator_host is not None and (
            "http://" in self._emulator_host or "https://" in self._emulator_host
        ):
            warnings.warn(_EMULATOR_HOST_HTTP_SCHEME)

    @property
    def credentials(self):
        """Getter for client's credentials.

        :rtype:
            :class:`Credentials <google.auth.credentials.Credentials>`
        :returns: The credentials stored on the client.
        """
        return self._credentials

    @property
    def project_name(self):
        """Project name to be used with Spanner APIs.

        .. note::

            This property will not change if ``project`` does not, but the
            return value is not cached.

        The project name is of the form

            ``"projects/{project}"``

        :rtype: str
        :returns: The project name to be used with the Cloud Spanner Admin
                  API RPC service.
        """
        return "projects/" + self.project

    @property
    def instance_admin_api(self):
        """Helper for session-related API calls."""
        if self._instance_admin_api is None:
            if self._emulator_host is not None:
                transport = instance_admin_grpc_transport.InstanceAdminGrpcTransport(
                    channel=grpc.insecure_channel(target=self._emulator_host)
                )
                self._instance_admin_api = InstanceAdminClient(
                    client_info=self._client_info,
                    client_options=self._client_options,
                    transport=transport,
                )
            else:
                self._instance_admin_api = InstanceAdminClient(
                    credentials=self.credentials,
                    client_info=self._client_info,
                    client_options=self._client_options,
                )
        return self._instance_admin_api

    @property
    def database_admin_api(self):
        """Helper for session-related API calls."""
        if self._database_admin_api is None:
            if self._emulator_host is not None:
                transport = database_admin_grpc_transport.DatabaseAdminGrpcTransport(
                    channel=grpc.insecure_channel(target=self._emulator_host)
                )
                self._database_admin_api = DatabaseAdminClient(
                    client_info=self._client_info,
                    client_options=self._client_options,
                    transport=transport,
                )
            else:
                self._database_admin_api = DatabaseAdminClient(
                    credentials=self.credentials,
                    client_info=self._client_info,
                    client_options=self._client_options,
                )
        return self._database_admin_api

    def copy(self):
        """Make a copy of this client.

        Copies the local data stored as simple types but does not copy the
        current state of any open connections with the Cloud Bigtable API.

        :rtype: :class:`.Client`
        :returns: A copy of the current client.
        """
        return self.__class__(project=self.project, credentials=self._credentials)

    def list_instance_configs(self, page_size=None, page_token=None):
        """List available instance configurations for the client's project.

        .. _RPC docs: https://cloud.google.com/spanner/docs/reference/rpc/\
                      google.spanner.admin.instance.v1#google.spanner.admin.\
                      instance.v1.InstanceAdmin.ListInstanceConfigs

        See `RPC docs`_.

        :type page_size: int
        :param page_size:
            Optional. The maximum number of configs in each page of results
            from this request. Non-positive values are ignored. Defaults
            to a sensible value set by the API.

        :type page_token: str
        :param page_token:
            Optional. If present, return the next batch of configs, using
            the value, which must correspond to the ``nextPageToken`` value
            returned in the previous response.  Deprecated: use the ``pages``
            property of the returned iterator instead of manually passing
            the token.

        :rtype: :class:`~google.api_core.page_iterator.Iterator`
        :returns:
            Iterator of
            :class:`~google.cloud.spanner_v1.instance.InstanceConfig`
            resources within the client's project.
        """
        metadata = _metadata_with_prefix(self.project_name)
        path = "projects/%s" % (self.project,)
        page_iter = self.instance_admin_api.list_instance_configs(
            path, page_size=page_size, metadata=metadata
        )
        page_iter.next_page_token = page_token
        page_iter.item_to_value = _item_to_instance_config
        return page_iter

    def instance(
        self,
        instance_id,
        configuration_name=None,
        display_name=None,
        node_count=DEFAULT_NODE_COUNT,
    ):
        """Factory to create a instance associated with this client.

        :type instance_id: str
        :param instance_id: The ID of the instance.

        :type configuration_name: string
        :param configuration_name:
           (Optional) Name of the instance configuration used to set up the
           instance's cluster, in the form:
           ``projects/<project>/instanceConfigs/``
           ``<config>``.
           **Required** for instances which do not yet exist.

        :type display_name: str
        :param display_name: (Optional) The display name for the instance in
                             the Cloud Console UI. (Must be between 4 and 30
                             characters.) If this value is not set in the
                             constructor, will fall back to the instance ID.

        :type node_count: int
        :param node_count: (Optional) The number of nodes in the instance's
                            cluster; used to set up the instance's cluster.

        :rtype: :class:`~google.cloud.spanner_v1.instance.Instance`
        :returns: an instance owned by this client.
        """
        return Instance(
            instance_id,
            self,
            configuration_name,
            node_count,
            display_name,
            self._emulator_host,
        )

    def list_instances(self, filter_="", page_size=None, page_token=None):
        """List instances for the client's project.

        See
        https://cloud.google.com/spanner/reference/rpc/google.spanner.admin.database.v1#google.spanner.admin.database.v1.InstanceAdmin.ListInstances

        :type filter_: string
        :param filter_: (Optional) Filter to select instances listed.  See
                        the ``ListInstancesRequest`` docs above for examples.

        :type page_size: int
        :param page_size:
            Optional. The maximum number of instances in each page of results
            from this request. Non-positive values are ignored. Defaults
            to a sensible value set by the API.

        :type page_token: str
        :param page_token:
            Optional. If present, return the next batch of instances, using
            the value, which must correspond to the ``nextPageToken`` value
            returned in the previous response.  Deprecated: use the ``pages``
            property of the returned iterator instead of manually passing
            the token.

        :rtype: :class:`~google.api_core.page_iterator.Iterator`
        :returns:
            Iterator of :class:`~google.cloud.spanner_v1.instance.Instance`
            resources within the client's project.
        """
        metadata = _metadata_with_prefix(self.project_name)
        path = "projects/%s" % (self.project,)
        page_iter = self.instance_admin_api.list_instances(
            path, filter_=filter_, page_size=page_size, metadata=metadata
        )
        page_iter.item_to_value = self._item_to_instance
        page_iter.next_page_token = page_token
        return page_iter

    def _item_to_instance(self, iterator, instance_pb):
        """Convert an instance protobuf to the native object.

        :type iterator: :class:`~google.api_core.page_iterator.Iterator`
        :param iterator: The iterator that is currently in use.

        :type instance_pb: :class:`~google.spanner.admin.instance.v1.Instance`
        :param instance_pb: An instance returned from the API.

        :rtype: :class:`~google.cloud.spanner_v1.instance.Instance`
        :returns: The next instance in the page.
        """
        return Instance.from_pb(instance_pb, self)


def _item_to_instance_config(iterator, config_pb):  # pylint: disable=unused-argument
    """Convert an instance config protobuf to the native object.

    :type iterator: :class:`~google.api_core.page_iterator.Iterator`
    :param iterator: The iterator that is currently in use.

    :type config_pb:
        :class:`~google.spanner.admin.instance.v1.InstanceConfig`
    :param config_pb: An instance config returned from the API.

    :rtype: :class:`~google.cloud.spanner_v1.instance.InstanceConfig`
    :returns: The next instance config in the page.
    """
    return InstanceConfig.from_pb(config_pb)
