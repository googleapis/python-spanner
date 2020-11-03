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

"""User friendly container for Cloud Spanner Instance."""

import google.api_core.operation
import re

from google.cloud.spanner_admin_instance_v1.proto import (
    spanner_instance_admin_pb2 as admin_v1_pb2,
)
from google.cloud.spanner_admin_database_v1.proto import (
    backup_pb2,
    spanner_database_admin_pb2,
)
from google.protobuf.empty_pb2 import Empty
from google.protobuf.field_mask_pb2 import FieldMask

# pylint: disable=ungrouped-imports
from google.cloud.exceptions import NotFound
from google.cloud.spanner_v1._helpers import _metadata_with_prefix
from google.cloud.spanner_v1.backup import Backup
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.pool import BurstyPool

# pylint: enable=ungrouped-imports


_INSTANCE_NAME_RE = re.compile(
    r"^projects/(?P<project>[^/]+)/" r"instances/(?P<instance_id>[a-z][-a-z0-9]*)$"
)

DEFAULT_NODE_COUNT = 1

_OPERATION_METADATA_MESSAGES = (
    backup_pb2.Backup,
    backup_pb2.CreateBackupMetadata,
    spanner_database_admin_pb2.CreateDatabaseMetadata,
    spanner_database_admin_pb2.Database,
    spanner_database_admin_pb2.OptimizeRestoredDatabaseMetadata,
    spanner_database_admin_pb2.RestoreDatabaseMetadata,
    spanner_database_admin_pb2.UpdateDatabaseDdlMetadata,
)

_OPERATION_METADATA_TYPES = {
    "type.googleapis.com/{}".format(message.DESCRIPTOR.full_name): message
    for message in _OPERATION_METADATA_MESSAGES
}

_OPERATION_RESPONSE_TYPES = {
    backup_pb2.CreateBackupMetadata: backup_pb2.Backup,
    spanner_database_admin_pb2.CreateDatabaseMetadata: spanner_database_admin_pb2.Database,
    spanner_database_admin_pb2.OptimizeRestoredDatabaseMetadata: spanner_database_admin_pb2.Database,
    spanner_database_admin_pb2.RestoreDatabaseMetadata: spanner_database_admin_pb2.Database,
    spanner_database_admin_pb2.UpdateDatabaseDdlMetadata: Empty,
}


def _type_string_to_type_pb(type_string):
    return _OPERATION_METADATA_TYPES.get(type_string, Empty)


class Instance(object):
    """Representation of a Cloud Spanner Instance.

    We can use a :class:`Instance` to:

    * :meth:`reload` itself
    * :meth:`create` itself
    * :meth:`update` itself
    * :meth:`delete` itself

    :type instance_id: str
    :param instance_id: The ID of the instance.

    :type client: :class:`~google.cloud.spanner_v1.client.Client`
    :param client: The client that owns the instance. Provides
                   authorization and a project ID.

    :type configuration_name: str
    :param configuration_name: Name of the instance configuration defining
                        how the instance will be created.
                        Required for instances which do not yet exist.

    :type node_count: int
    :param node_count: (Optional) Number of nodes allocated to the instance.

    :type display_name: str
    :param display_name: (Optional) The display name for the instance in the
                         Cloud Console UI. (Must be between 4 and 30
                         characters.) If this value is not set in the
                         constructor, will fall back to the instance ID.
    """

    def __init__(
        self,
        instance_id,
        client,
        configuration_name=None,
        node_count=DEFAULT_NODE_COUNT,
        display_name=None,
        emulator_host=None,
    ):
        self.instance_id = instance_id
        self._client = client
        self.configuration_name = configuration_name
        self.node_count = node_count
        self.display_name = display_name or instance_id
        self.emulator_host = emulator_host

    def _update_from_pb(self, instance_pb):
        """Refresh self from the server-provided protobuf.

        Helper for :meth:`from_pb` and :meth:`reload`.
        """
        if not instance_pb.display_name:  # Simple field (string)
            raise ValueError("Instance protobuf does not contain display_name")
        self.display_name = instance_pb.display_name
        self.configuration_name = instance_pb.config
        self.node_count = instance_pb.node_count

    @classmethod
    def from_pb(cls, instance_pb, client):
        """Creates an instance from a protobuf.

        :type instance_pb:
            :class:`~google.spanner.v2.spanner_instance_admin_pb2.Instance`
        :param instance_pb: A instance protobuf object.

        :type client: :class:`~google.cloud.spanner_v1.client.Client`
        :param client: The client that owns the instance.

        :rtype: :class:`Instance`
        :returns: The instance parsed from the protobuf response.
        :raises ValueError:
            if the instance name does not match
            ``projects/{project}/instances/{instance_id}`` or if the parsed
            project ID does not match the project ID on the client.
        """
        match = _INSTANCE_NAME_RE.match(instance_pb.name)
        if match is None:
            raise ValueError(
                "Instance protobuf name was not in the " "expected format.",
                instance_pb.name,
            )
        if match.group("project") != client.project:
            raise ValueError(
                "Project ID on instance does not match the " "project ID on the client"
            )
        instance_id = match.group("instance_id")
        configuration_name = instance_pb.config

        result = cls(instance_id, client, configuration_name)
        result._update_from_pb(instance_pb)
        return result

    @property
    def name(self):
        """Instance name used in requests.

        .. note::

           This property will not change if ``instance_id`` does not,
           but the return value is not cached.

        The instance name is of the form

            ``"projects/{project}/instances/{instance_id}"``

        :rtype: str
        :returns: The instance name.
        """
        return self._client.project_name + "/instances/" + self.instance_id

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        # NOTE: This does not compare the configuration values, such as
        #       the display_name. Instead, it only compares
        #       identifying values instance ID and client. This is
        #       intentional, since the same instance can be in different states
        #       if not synchronized. Instances with similar instance
        #       settings but different clients can't be used in the same way.
        return other.instance_id == self.instance_id and other._client == self._client

    def __ne__(self, other):
        return not self == other

    def copy(self):
        """Make a copy of this instance.

        Copies the local data stored as simple types and copies the client
        attached to this instance.

        :rtype: :class:`~google.cloud.spanner_v1.instance.Instance`
        :returns: A copy of the current instance.
        """
        new_client = self._client.copy()
        return self.__class__(
            self.instance_id,
            new_client,
            self.configuration_name,
            node_count=self.node_count,
            display_name=self.display_name,
        )

    def create(self):
        """Create this instance.

        See
        https://cloud.google.com/spanner/reference/rpc/google.spanner.admin.instance.v1#google.spanner.admin.instance.v1.InstanceAdmin.CreateInstance

        .. note::

           Uses the ``project`` and ``instance_id`` on the current
           :class:`Instance` in addition to the ``display_name``.
           To change them before creating, reset the values via

           .. code:: python

              instance.display_name = 'New display name'
              instance.instance_id = 'i-changed-my-mind'

           before calling :meth:`create`.

        :rtype: :class:`~google.api_core.operation.Operation`
        :returns: an operation instance
        :raises Conflict: if the instance already exists
        """
        api = self._client.instance_admin_api
        instance_pb = admin_v1_pb2.Instance(
            name=self.name,
            config=self.configuration_name,
            display_name=self.display_name,
            node_count=self.node_count,
        )
        metadata = _metadata_with_prefix(self.name)

        future = api.create_instance(
            parent=self._client.project_name,
            instance_id=self.instance_id,
            instance=instance_pb,
            metadata=metadata,
        )

        return future

    def exists(self):
        """Test whether this instance exists.

        See
        https://cloud.google.com/spanner/reference/rpc/google.spanner.admin.instance.v1#google.spanner.admin.instance.v1.InstanceAdmin.GetInstanceConfig

        :rtype: bool
        :returns: True if the instance exists, else false
        """
        api = self._client.instance_admin_api
        metadata = _metadata_with_prefix(self.name)

        try:
            api.get_instance(self.name, metadata=metadata)
        except NotFound:
            return False

        return True

    def reload(self):
        """Reload the metadata for this instance.

        See
        https://cloud.google.com/spanner/reference/rpc/google.spanner.admin.instance.v1#google.spanner.admin.instance.v1.InstanceAdmin.GetInstanceConfig

        :raises NotFound: if the instance does not exist
        """
        api = self._client.instance_admin_api
        metadata = _metadata_with_prefix(self.name)

        instance_pb = api.get_instance(self.name, metadata=metadata)

        self._update_from_pb(instance_pb)

    def update(self):
        """Update this instance.

        See
        https://cloud.google.com/spanner/reference/rpc/google.spanner.admin.instance.v1#google.spanner.admin.instance.v1.InstanceAdmin.UpdateInstance

        .. note::

            Updates the ``display_name`` and ``node_count``. To change those
            values before updating, set them via

            .. code:: python

                instance.display_name = 'New display name'
                instance.node_count = 5

           before calling :meth:`update`.

        :rtype: :class:`google.api_core.operation.Operation`
        :returns: an operation instance
        :raises NotFound: if the instance does not exist
        """
        api = self._client.instance_admin_api
        instance_pb = admin_v1_pb2.Instance(
            name=self.name,
            config=self.configuration_name,
            display_name=self.display_name,
            node_count=self.node_count,
        )
        field_mask = FieldMask(paths=["config", "display_name", "node_count"])
        metadata = _metadata_with_prefix(self.name)

        future = api.update_instance(
            instance=instance_pb, field_mask=field_mask, metadata=metadata
        )

        return future

    def delete(self):
        """Mark an instance and all of its databases for permanent deletion.

        See
        https://cloud.google.com/spanner/reference/rpc/google.spanner.admin.instance.v1#google.spanner.admin.instance.v1.InstanceAdmin.DeleteInstance

        Immediately upon completion of the request:

        * Billing will cease for all of the instance's reserved resources.

        Soon afterward:

        * The instance and all databases within the instance will be deleteed.
          All data in the databases will be permanently deleted.
        """
        api = self._client.instance_admin_api
        metadata = _metadata_with_prefix(self.name)

        api.delete_instance(self.name, metadata=metadata)

    def database(self, database_id, ddl_statements=(), pool=None):
        """Factory to create a database within this instance.

        :type database_id: str
        :param database_id: The ID of the instance.

        :type ddl_statements: list of string
        :param ddl_statements: (Optional) DDL statements, excluding the
                               'CREATE DATABSE' statement.

        :type pool: concrete subclass of
                    :class:`~google.cloud.spanner_v1.pool.AbstractSessionPool`.
        :param pool: (Optional) session pool to be used by database.

        :rtype: :class:`~google.cloud.spanner_v1.database.Database`
        :returns: a database owned by this instance.
        """
        return Database(database_id, self, ddl_statements=ddl_statements, pool=pool)

    def list_databases(self, page_size=None, page_token=None):
        """List databases for the instance.

        See
        https://cloud.google.com/spanner/reference/rpc/google.spanner.admin.database.v1#google.spanner.admin.database.v1.DatabaseAdmin.ListDatabases

        :type page_size: int
        :param page_size:
            Optional. The maximum number of databases in each page of results
            from this request. Non-positive values are ignored. Defaults
            to a sensible value set by the API.

        :type page_token: str
        :param page_token:
            Optional. If present, return the next batch of databases, using
            the value, which must correspond to the ``nextPageToken`` value
            returned in the previous response.  Deprecated: use the ``pages``
            property of the returned iterator instead of manually passing
            the token.

        :rtype: :class:`~google.api._ore.page_iterator.Iterator`
        :returns:
            Iterator of :class:`~google.cloud.spanner_v1.database.Database`
            resources within the current instance.
        """
        metadata = _metadata_with_prefix(self.name)
        page_iter = self._client.database_admin_api.list_databases(
            self.name, page_size=page_size, metadata=metadata
        )
        page_iter.next_page_token = page_token
        page_iter.item_to_value = self._item_to_database
        return page_iter

    def _item_to_database(self, iterator, database_pb):
        """Convert a database protobuf to the native object.

        :type iterator: :class:`~google.api_core.page_iterator.Iterator`
        :param iterator: The iterator that is currently in use.

        :type database_pb: :class:`~google.spanner.admin.database.v1.Database`
        :param database_pb: A database returned from the API.

        :rtype: :class:`~google.cloud.spanner_v1.database.Database`
        :returns: The next database in the page.
        """
        return Database.from_pb(database_pb, self, pool=BurstyPool())

    def backup(self, backup_id, database="", expire_time=None):
        """Factory to create a backup within this instance.

        :type backup_id: str
        :param backup_id: The ID of the backup.

        :type database: :class:`~google.cloud.spanner_v1.database.Database`
        :param database:
            Optional. The database that will be used when creating the backup.
            Required if the create method needs to be called.

        :type expire_time: :class:`datetime.datetime`
        :param expire_time:
            Optional. The expire time that will be used when creating the backup.
            Required if the create method needs to be called.
        """
        try:
            return Backup(
                backup_id, self, database=database.name, expire_time=expire_time
            )
        except AttributeError:
            return Backup(backup_id, self, database=database, expire_time=expire_time)

    def list_backups(self, filter_="", page_size=None):
        """List backups for the instance.

        :type filter_: str
        :param filter_:
            Optional. A string specifying a filter for which backups to list.

        :type page_size: int
        :param page_size:
            Optional. The maximum number of databases in each page of results
            from this request. Non-positive values are ignored. Defaults to a
            sensible value set by the API.

        :rtype: :class:`~google.api_core.page_iterator.Iterator`
        :returns:
            Iterator of :class:`~google.cloud.spanner_v1.backup.Backup`
            resources within the current instance.
        """
        metadata = _metadata_with_prefix(self.name)
        page_iter = self._client.database_admin_api.list_backups(
            self.name, filter_, page_size=page_size, metadata=metadata
        )
        page_iter.item_to_value = self._item_to_backup
        return page_iter

    def _item_to_backup(self, iterator, backup_pb):
        """Convert a backup protobuf to the native object.

        :type iterator: :class:`~google.api_core.page_iterator.Iterator`
        :param iterator: The iterator that is currently in use.

        :type backup_pb: :class:`~google.spanner.admin.database.v1.Backup`
        :param backup_pb: A backup returned from the API.

        :rtype: :class:`~google.cloud.spanner_v1.backup.Backup`
        :returns: The next backup in the page.
        """
        return Backup.from_pb(backup_pb, self)

    def list_backup_operations(self, filter_="", page_size=None):
        """List backup operations for the instance.

        :type filter_: str
        :param filter_:
            Optional. A string specifying a filter for which backup operations
            to list.

        :type page_size: int
        :param page_size:
            Optional. The maximum number of operations in each page of results
            from this request. Non-positive values are ignored. Defaults to a
            sensible value set by the API.

        :rtype: :class:`~google.api_core.page_iterator.Iterator`
        :returns:
            Iterator of :class:`~google.api_core.operation.Operation`
            resources within the current instance.
        """
        metadata = _metadata_with_prefix(self.name)
        page_iter = self._client.database_admin_api.list_backup_operations(
            self.name, filter_, page_size=page_size, metadata=metadata
        )
        page_iter.item_to_value = self._item_to_operation
        return page_iter

    def list_database_operations(self, filter_="", page_size=None):
        """List database operations for the instance.

        :type filter_: str
        :param filter_:
            Optional. A string specifying a filter for which database operations
            to list.

        :type page_size: int
        :param page_size:
            Optional. The maximum number of operations in each page of results
            from this request. Non-positive values are ignored. Defaults to a
            sensible value set by the API.

        :rtype: :class:`~google.api_core.page_iterator.Iterator`
        :returns:
            Iterator of :class:`~google.api_core.operation.Operation`
            resources within the current instance.
        """
        metadata = _metadata_with_prefix(self.name)
        page_iter = self._client.database_admin_api.list_database_operations(
            self.name, filter_, page_size=page_size, metadata=metadata
        )
        page_iter.item_to_value = self._item_to_operation
        return page_iter

    def _item_to_operation(self, iterator, operation_pb):
        """Convert an operation protobuf to the native object.

        :type iterator: :class:`~google.api_core.page_iterator.Iterator`
        :param iterator: The iterator that is currently in use.

        :type operation_pb: :class:`~google.longrunning.operations.Operation`
        :param operation_pb: An operation returned from the API.

        :rtype: :class:`~google.api_core.operation.Operation`
        :returns: The next operation in the page.
        """
        operations_client = self._client.database_admin_api.transport._operations_client
        metadata_type = _type_string_to_type_pb(operation_pb.metadata.type_url)
        response_type = _OPERATION_RESPONSE_TYPES[metadata_type]
        return google.api_core.operation.from_gapic(
            operation_pb, operations_client, response_type, metadata_type=metadata_type
        )
