# -*- coding: utf-8 -*-
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
#
from google.cloud.spanner_admin_database import gapic_version as package_version

__version__ = package_version.__version__


from google.cloud.spanner_admin_database_v1.services.database_admin.client import DatabaseAdminClient
from google.cloud.spanner_admin_database_v1.services.database_admin.async_client import DatabaseAdminAsyncClient

from google.cloud.spanner_admin_database_v1.types.backup import Backup
from google.cloud.spanner_admin_database_v1.types.backup import BackupInfo
from google.cloud.spanner_admin_database_v1.types.backup import BackupInstancePartition
from google.cloud.spanner_admin_database_v1.types.backup import CopyBackupEncryptionConfig
from google.cloud.spanner_admin_database_v1.types.backup import CopyBackupMetadata
from google.cloud.spanner_admin_database_v1.types.backup import CopyBackupRequest
from google.cloud.spanner_admin_database_v1.types.backup import CreateBackupEncryptionConfig
from google.cloud.spanner_admin_database_v1.types.backup import CreateBackupMetadata
from google.cloud.spanner_admin_database_v1.types.backup import CreateBackupRequest
from google.cloud.spanner_admin_database_v1.types.backup import DeleteBackupRequest
from google.cloud.spanner_admin_database_v1.types.backup import FullBackupSpec
from google.cloud.spanner_admin_database_v1.types.backup import GetBackupRequest
from google.cloud.spanner_admin_database_v1.types.backup import IncrementalBackupSpec
from google.cloud.spanner_admin_database_v1.types.backup import ListBackupOperationsRequest
from google.cloud.spanner_admin_database_v1.types.backup import ListBackupOperationsResponse
from google.cloud.spanner_admin_database_v1.types.backup import ListBackupsRequest
from google.cloud.spanner_admin_database_v1.types.backup import ListBackupsResponse
from google.cloud.spanner_admin_database_v1.types.backup import UpdateBackupRequest
from google.cloud.spanner_admin_database_v1.types.backup_schedule import BackupSchedule
from google.cloud.spanner_admin_database_v1.types.backup_schedule import BackupScheduleSpec
from google.cloud.spanner_admin_database_v1.types.backup_schedule import CreateBackupScheduleRequest
from google.cloud.spanner_admin_database_v1.types.backup_schedule import CrontabSpec
from google.cloud.spanner_admin_database_v1.types.backup_schedule import DeleteBackupScheduleRequest
from google.cloud.spanner_admin_database_v1.types.backup_schedule import GetBackupScheduleRequest
from google.cloud.spanner_admin_database_v1.types.backup_schedule import ListBackupSchedulesRequest
from google.cloud.spanner_admin_database_v1.types.backup_schedule import ListBackupSchedulesResponse
from google.cloud.spanner_admin_database_v1.types.backup_schedule import UpdateBackupScheduleRequest
from google.cloud.spanner_admin_database_v1.types.common import EncryptionConfig
from google.cloud.spanner_admin_database_v1.types.common import EncryptionInfo
from google.cloud.spanner_admin_database_v1.types.common import OperationProgress
from google.cloud.spanner_admin_database_v1.types.common import DatabaseDialect
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import AddSplitPointsRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import AddSplitPointsResponse
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import CreateDatabaseMetadata
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import CreateDatabaseRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import Database
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import DatabaseRole
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import DdlStatementActionInfo
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import DropDatabaseRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import GetDatabaseDdlRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import GetDatabaseDdlResponse
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import GetDatabaseRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import InternalUpdateGraphOperationRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import InternalUpdateGraphOperationResponse
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import ListDatabaseOperationsRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import ListDatabaseOperationsResponse
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import ListDatabaseRolesRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import ListDatabaseRolesResponse
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import ListDatabasesRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import ListDatabasesResponse
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import OptimizeRestoredDatabaseMetadata
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import RestoreDatabaseEncryptionConfig
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import RestoreDatabaseMetadata
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import RestoreDatabaseRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import RestoreInfo
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import SplitPoints
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import UpdateDatabaseDdlMetadata
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import UpdateDatabaseDdlRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import UpdateDatabaseMetadata
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import UpdateDatabaseRequest
from google.cloud.spanner_admin_database_v1.types.spanner_database_admin import RestoreSourceType

__all__ = ('DatabaseAdminClient',
    'DatabaseAdminAsyncClient',
    'Backup',
    'BackupInfo',
    'BackupInstancePartition',
    'CopyBackupEncryptionConfig',
    'CopyBackupMetadata',
    'CopyBackupRequest',
    'CreateBackupEncryptionConfig',
    'CreateBackupMetadata',
    'CreateBackupRequest',
    'DeleteBackupRequest',
    'FullBackupSpec',
    'GetBackupRequest',
    'IncrementalBackupSpec',
    'ListBackupOperationsRequest',
    'ListBackupOperationsResponse',
    'ListBackupsRequest',
    'ListBackupsResponse',
    'UpdateBackupRequest',
    'BackupSchedule',
    'BackupScheduleSpec',
    'CreateBackupScheduleRequest',
    'CrontabSpec',
    'DeleteBackupScheduleRequest',
    'GetBackupScheduleRequest',
    'ListBackupSchedulesRequest',
    'ListBackupSchedulesResponse',
    'UpdateBackupScheduleRequest',
    'EncryptionConfig',
    'EncryptionInfo',
    'OperationProgress',
    'DatabaseDialect',
    'AddSplitPointsRequest',
    'AddSplitPointsResponse',
    'CreateDatabaseMetadata',
    'CreateDatabaseRequest',
    'Database',
    'DatabaseRole',
    'DdlStatementActionInfo',
    'DropDatabaseRequest',
    'GetDatabaseDdlRequest',
    'GetDatabaseDdlResponse',
    'GetDatabaseRequest',
    'InternalUpdateGraphOperationRequest',
    'InternalUpdateGraphOperationResponse',
    'ListDatabaseOperationsRequest',
    'ListDatabaseOperationsResponse',
    'ListDatabaseRolesRequest',
    'ListDatabaseRolesResponse',
    'ListDatabasesRequest',
    'ListDatabasesResponse',
    'OptimizeRestoredDatabaseMetadata',
    'RestoreDatabaseEncryptionConfig',
    'RestoreDatabaseMetadata',
    'RestoreDatabaseRequest',
    'RestoreInfo',
    'SplitPoints',
    'UpdateDatabaseDdlMetadata',
    'UpdateDatabaseDdlRequest',
    'UpdateDatabaseMetadata',
    'UpdateDatabaseRequest',
    'RestoreSourceType',
)
