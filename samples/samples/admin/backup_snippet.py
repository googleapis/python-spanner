# Copyright 2024 Google Inc. All Rights Reserved.
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

"""This application demonstrates how to create and restore from backups
using Cloud Spanner.
For more information, see the README.rst under /spanner.
"""

import time
from datetime import datetime, timedelta

from google.api_core import protobuf_helpers
from google.cloud import spanner
from google.cloud.exceptions import NotFound
from google.cloud.spanner_admin_database_v1.types import backup as backup_pb
from google.cloud.spanner_admin_database_v1.types import spanner_database_admin


# [START spanner_create_backup]
def create_backup(instance_id, database_id, backup_id, version_time):
    """Creates a backup for a database."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Create a backup
    expire_time = datetime.utcnow() + timedelta(days=14)

    request = backup_pb.CreateBackupRequest(
        parent=instance.name,
        backup_id=backup_id,
        backup=backup_pb.Backup(
            database=database.name,
            expire_time=expire_time,
            version_time=version_time,
        ),
    )

    operation = spanner_client.database_admin_api.create_backup(request)

    # Wait for backup operation to complete.
    backup = operation.result(2100)

    # Verify that the backup is ready.
    assert backup.state == backup_pb.Backup.State.READY

    print(
        "Backup {} of size {} bytes was created at {} for version of database at {}".format(
            backup.name, backup.size_bytes, backup.create_time, backup.version_time
        )
    )


# [END spanner_create_backup]


# [START spanner_create_backup_with_encryption_key]
def create_backup_with_encryption_key(
    instance_id, database_id, backup_id, kms_key_name
):
    """Creates a backup for a database using a Customer Managed Encryption Key (CMEK)."""
    from google.cloud.spanner_admin_database_v1 import \
        CreateBackupEncryptionConfig

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Create a backup
    expire_time = datetime.utcnow() + timedelta(days=14)
    encryption_config = {
        "encryption_type": CreateBackupEncryptionConfig.EncryptionType.CUSTOMER_MANAGED_ENCRYPTION,
        "kms_key_name": kms_key_name,
    }
    request = backup_pb.CreateBackupRequest(
        parent=instance.name,
        backup_id=backup_id,
        backup=backup_pb.Backup(
            database=database.name,
            expire_time=expire_time,
        ),
        encryption_config=encryption_config,
    )
    operation = spanner_client.database_admin_api.create_backup(request)

    # Wait for backup operation to complete.
    backup = operation.result(2100)

    # Verify that the backup is ready.
    assert backup.state == backup_pb.Backup.State.READY

    # Get the name, create time, backup size and encryption key.
    print(
        "Backup {} of size {} bytes was created at {} using encryption key {}".format(
            backup.name, backup.size_bytes, backup.create_time, kms_key_name
        )
    )


# [END spanner_create_backup_with_encryption_key]


# [START spanner_restore_backup]
def restore_database(instance_id, new_database_id, backup_id):
    """Restores a database from a backup."""
    from google.cloud.spanner_admin_database_v1 import RestoreDatabaseRequest

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    # Start restoring an existing backup to a new database.
    request = RestoreDatabaseRequest(
        parent=instance.name,
        database_id=new_database_id,
        backup="{}/backups/{}".format(instance.name, backup_id),
    )
    operation = spanner_client.database_admin_api.restore_database(request)

    # Wait for restore operation to complete.
    db = operation.result(1600)

    # Newly created database has restore information.
    restore_info = db.restore_info
    print(
        "Database {} restored to {} from backup {} with version time {}.".format(
            restore_info.backup_info.source_database,
            new_database_id,
            restore_info.backup_info.backup,
            restore_info.backup_info.version_time,
        )
    )


# [END spanner_restore_backup]


# [START spanner_restore_backup_with_encryption_key]
def restore_database_with_encryption_key(
    instance_id, new_database_id, backup_id, kms_key_name
):
    """Restores a database from a backup using a Customer Managed Encryption Key (CMEK)."""
    from google.cloud.spanner_admin_database_v1 import (
        RestoreDatabaseEncryptionConfig, RestoreDatabaseRequest)

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    # Start restoring an existing backup to a new database.
    encryption_config = {
        "encryption_type": RestoreDatabaseEncryptionConfig.EncryptionType.CUSTOMER_MANAGED_ENCRYPTION,
        "kms_key_name": kms_key_name,
    }
    new_database = instance.database(
        new_database_id, encryption_config=encryption_config
    )

    request = RestoreDatabaseRequest(
        parent=instance.name,
        database_id=new_database_id,
        backup="{}/backups/{}".format(instance.name, backup_id),
        encryption_config=encryption_config,
    )
    operation = spanner_client.database_admin_api.restore_database(request)

    # Wait for restore operation to complete.
    db = operation.result(1600)

    # Newly created database has restore information.
    restore_info = db.restore_info
    print(
        "Database {} restored to {} from backup {} with using encryption key {}.".format(
            restore_info.backup_info.source_database,
            new_database_id,
            restore_info.backup_info.backup,
            restore_info.encryption_config.kms_key_name,
        )
    )


# [END spanner_restore_backup_with_encryption_key]


# [START spanner_cancel_backup_create]
def cancel_backup(instance_id, database_id, backup_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    expire_time = datetime.utcnow() + timedelta(days=30)

    # Create a backup.
    request = backup_pb.CreateBackupRequest(
        parent=instance.name,
        backup_id=backup_id,
        backup=backup_pb.Backup(
            database=database.name,
            expire_time=expire_time,
        ),
    )

    operation = spanner_client.database_admin_api.create_backup(request)
    # Cancel backup creation.
    operation.cancel()

    # Cancel operations are best effort so either it will complete or
    # be cancelled.
    while not operation.done():
        time.sleep(300)  # 5 mins

    try:
        spanner_client.database_admin_api.get_backup(
            backup_pb.GetBackupRequest(
                name="{}/backups/{}".format(instance.name, backup_id)
            )
        )
    except NotFound:
        print("Backup creation was successfully cancelled.")
        return
    print("Backup was created before the cancel completed.")
    spanner_client.database_admin_api.delete_backup(
        backup_pb.DeleteBackupRequest(
            name="{}/backups/{}".format(instance.name, backup_id)
        )
    )
    print("Backup deleted.")


# [END spanner_cancel_backup_create]


# [START spanner_list_backup_operations]
def list_backup_operations(instance_id, database_id, backup_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    # List the CreateBackup operations.
    filter_ = (
        "(metadata.@type:type.googleapis.com/"
        "google.spanner.admin.database.v1.CreateBackupMetadata) "
        "AND (metadata.database:{})"
    ).format(database_id)
    request = backup_pb.ListBackupOperationsRequest(
        parent=instance.name, filter=filter_
    )
    operations = spanner_client.database_admin_api.list_backup_operations(request)
    for op in operations:
        metadata = protobuf_helpers.from_any_pb(
            backup_pb.CreateBackupMetadata, op.metadata
        )
        print(
            "Backup {} on database {}: {}% complete.".format(
                metadata.name, metadata.database, metadata.progress.progress_percent
            )
        )

    # List the CopyBackup operations.
    filter_ = (
        "(metadata.@type:type.googleapis.com/google.spanner.admin.database.v1.CopyBackupMetadata) "
        "AND (metadata.source_backup:{})"
    ).format(backup_id)
    request = backup_pb.ListBackupOperationsRequest(
        parent=instance.name, filter=filter_
    )
    operations = spanner_client.database_admin_api.list_backup_operations(request)
    for op in operations:
        metadata = protobuf_helpers.from_any_pb(
            backup_pb.CopyBackupMetadata, op.metadata
        )
        print(
            "Backup {} on source backup {}: {}% complete.".format(
                metadata.name,
                metadata.source_backup,
                metadata.progress.progress_percent,
            )
        )


# [END spanner_list_backup_operations]


# [START spanner_list_database_operations]
def list_database_operations(instance_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    # List the progress of restore.
    filter_ = (
        "(metadata.@type:type.googleapis.com/"
        "google.spanner.admin.database.v1.OptimizeRestoredDatabaseMetadata)"
    )
    request = spanner_database_admin.ListDatabaseOperationsRequest(
        parent=instance.name, filter=filter_
    )
    operations = spanner_client.database_admin_api.list_database_operations(request)
    for op in operations:
        metadata = protobuf_helpers.from_any_pb(
            spanner_database_admin.OptimizeRestoredDatabaseMetadata, op.metadata
        )
        print(
            "Database {} restored from backup is {}% optimized.".format(
                metadata.name, metadata.progress.progress_percent
            )
        )


# [END spanner_list_database_operations]


# [START spanner_list_backups]
def list_backups(instance_id, database_id, backup_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    # List all backups.
    print("All backups:")
    request = backup_pb.ListBackupsRequest(parent=instance.name, filter="")
    operations = spanner_client.database_admin_api.list_backups(request)
    for backup in operations:
        print(backup.name)

    # List all backups that contain a name.
    print('All backups with backup name containing "{}":'.format(backup_id))
    request = backup_pb.ListBackupsRequest(
        parent=instance.name, filter="name:{}".format(backup_id)
    )
    operations = spanner_client.database_admin_api.list_backups(request)
    for backup in operations:
        print(backup.name)

    # List all backups for a database that contains a name.
    print('All backups with database name containing "{}":'.format(database_id))
    request = backup_pb.ListBackupsRequest(
        parent=instance.name, filter="database:{}".format(database_id)
    )
    operations = spanner_client.database_admin_api.list_backups(request)
    for backup in operations:
        print(backup.name)

    # List all backups that expire before a timestamp.
    expire_time = datetime.utcnow().replace(microsecond=0) + timedelta(days=30)
    print(
        'All backups with expire_time before "{}-{}-{}T{}:{}:{}Z":'.format(
            *expire_time.timetuple()
        )
    )
    request = backup_pb.ListBackupsRequest(
        parent=instance.name,
        filter='expire_time < "{}-{}-{}T{}:{}:{}Z"'.format(*expire_time.timetuple()),
    )
    operations = spanner_client.database_admin_api.list_backups(request)
    for backup in operations:
        print(backup.name)

    # List all backups with a size greater than some bytes.
    print("All backups with backup size more than 100 bytes:")
    request = backup_pb.ListBackupsRequest(
        parent=instance.name, filter="size_bytes > 100"
    )
    operations = spanner_client.database_admin_api.list_backups(request)
    for backup in operations:
        print(backup.name)

    # List backups that were created after a timestamp that are also ready.
    create_time = datetime.utcnow().replace(microsecond=0) - timedelta(days=1)
    print(
        'All backups created after "{}-{}-{}T{}:{}:{}Z" and are READY:'.format(
            *create_time.timetuple()
        )
    )
    request = backup_pb.ListBackupsRequest(
        parent=instance.name,
        filter='create_time >= "{}-{}-{}T{}:{}:{}Z" AND state:READY'.format(
            *create_time.timetuple()
        ),
    )
    operations = spanner_client.database_admin_api.list_backups(request)
    for backup in operations:
        print(backup.name)

    print("All backups with pagination")
    # If there are multiple pages, additional ``ListBackup``
    # requests will be made as needed while iterating.
    paged_backups = set()
    request = backup_pb.ListBackupsRequest(parent=instance.name, page_size=2)
    operations = spanner_client.database_admin_api.list_backups(request)
    for backup in operations:
        paged_backups.add(backup.name)
    for backup in paged_backups:
        print(backup)


# [END spanner_list_backups]


# [START spanner_delete_backup]
def delete_backup(instance_id, backup_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    backup = spanner_client.database_admin_api.get_backup(
        backup_pb.GetBackupRequest(
            name="{}/backups/{}".format(instance.name, backup_id)
        )
    )

    # Wait for databases that reference this backup to finish optimizing.
    while backup.referencing_databases:
        time.sleep(30)
        backup = spanner_client.database_admin_api.get_backup(
            backup_pb.GetBackupRequest(
                name="{}/backups/{}".format(instance.name, backup_id)
            )
        )

    # Delete the backup.
    spanner_client.database_admin_api.delete_backup(
        backup_pb.DeleteBackupRequest(name=backup.name)
    )

    # Verify that the backup is deleted.
    try:
        backup = spanner_client.database_admin_api.get_backup(
            backup_pb.GetBackupRequest(name=backup.name)
        )
    except NotFound:
        print("Backup {} has been deleted.".format(backup.name))
        return
    assert True is False


# [END spanner_delete_backup]


# [START spanner_update_backup]
def update_backup(instance_id, backup_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    backup = spanner_client.database_admin_api.get_backup(
        backup_pb.GetBackupRequest(
            name="{}/backups/{}".format(instance.name, backup_id)
        )
    )

    # Expire time must be within 366 days of the create time of the backup.
    old_expire_time = backup.expire_time
    # New expire time should be less than the max expire time
    new_expire_time = min(backup.max_expire_time, old_expire_time + timedelta(days=30))
    spanner_client.database_admin_api.update_backup(
        backup_pb.UpdateBackupRequest(
            backup=backup_pb.Backup(name=backup.name, expire_time=new_expire_time),
            update_mask={"paths": ["expire_time"]},
        )
    )
    print(
        "Backup {} expire time was updated from {} to {}.".format(
            backup.name, old_expire_time, new_expire_time
        )
    )


# [END spanner_update_backup]


# [START spanner_create_database_with_version_retention_period]
def create_database_with_version_retention_period(
    instance_id, database_id, retention_period
):
    """Creates a database with a version retention period."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    ddl_statements = [
        "CREATE TABLE Singers ("
        + "  SingerId   INT64 NOT NULL,"
        + "  FirstName  STRING(1024),"
        + "  LastName   STRING(1024),"
        + "  SingerInfo BYTES(MAX)"
        + ") PRIMARY KEY (SingerId)",
        "CREATE TABLE Albums ("
        + "  SingerId     INT64 NOT NULL,"
        + "  AlbumId      INT64 NOT NULL,"
        + "  AlbumTitle   STRING(MAX)"
        + ") PRIMARY KEY (SingerId, AlbumId),"
        + "  INTERLEAVE IN PARENT Singers ON DELETE CASCADE",
        "ALTER DATABASE `{}`"
        " SET OPTIONS (version_retention_period = '{}')".format(
            database_id, retention_period
        ),
    ]
    operation = spanner_client.database_admin_api.create_database(
        request=spanner_database_admin.CreateDatabaseRequest(
            parent=instance.name,
            create_statement="CREATE DATABASE `{}`".format(database_id),
            extra_statements=ddl_statements,
        )
    )

    db = operation.result(30)
    print(
        "Database {} created with version retention period {} and earliest version time {}".format(
            db.name, db.version_retention_period, db.earliest_version_time
        )
    )

    spanner_client.database_admin_api.drop_database(
        spanner_database_admin.DropDatabaseRequest(database=db.name)
    )


# [END spanner_create_database_with_version_retention_period]


# [START spanner_copy_backup]
def copy_backup(instance_id, backup_id, source_backup_path):
    """Copies a backup."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    # Create a backup object and wait for copy backup operation to complete.
    expire_time = datetime.utcnow() + timedelta(days=14)
    request = backup_pb.CopyBackupRequest(
        parent=instance.name,
        backup_id=backup_id,
        source_backup=source_backup_path,
        expire_time=expire_time,
    )

    operation = spanner_client.database_admin_api.copy_backup(request)

    # Wait for backup operation to complete.
    copy_backup = operation.result(2100)

    # Verify that the copy backup is ready.
    assert copy_backup.state == backup_pb.Backup.State.READY

    print(
        "Backup {} of size {} bytes was created at {} with version time {}".format(
            copy_backup.name,
            copy_backup.size_bytes,
            copy_backup.create_time,
            copy_backup.version_time,
        )
    )


# [END spanner_copy_backup]
