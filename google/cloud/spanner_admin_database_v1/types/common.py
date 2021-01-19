# -*- coding: utf-8 -*-

# Copyright 2020 Google LLC
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

import proto  # type: ignore


from google.protobuf import timestamp_pb2 as timestamp  # type: ignore
from google.rpc import status_pb2 as status  # type: ignore


__protobuf__ = proto.module(
    package="google.spanner.admin.database.v1",
    manifest={"OperationProgress", "EncryptionConfig", "EncryptionInfo",},
)


class OperationProgress(proto.Message):
    r"""Encapsulates progress related information for a Cloud Spanner
    long running operation.

    Attributes:
        progress_percent (int):
            Percent completion of the operation.
            Values are between 0 and 100 inclusive.
        start_time (google.protobuf.timestamp_pb2.Timestamp):
            Time the request was received.
        end_time (google.protobuf.timestamp_pb2.Timestamp):
            If set, the time at which this operation
            failed or was completed successfully.
    """

    progress_percent = proto.Field(proto.INT32, number=1)

    start_time = proto.Field(proto.MESSAGE, number=2, message=timestamp.Timestamp,)

    end_time = proto.Field(proto.MESSAGE, number=3, message=timestamp.Timestamp,)


class EncryptionConfig(proto.Message):
    r"""Encryption configuration describing key resources in Cloud
    KMS used to encrypt/decrypt a Cloud Spanner database.

    Attributes:
        kms_key_name (str):
            The resource name of the Cloud KMS key that was used to
            encrypt and decrypt the database. The form of the
            kms_key_name is
            ``projects/<project>/locations/<location>/keyRings/<key_ring>/cryptoKeys\ /<kms_key_name>``.
            api-linter: core::0122::name-suffix=disabled
            aip.dev/not-precedent: crypto key identifiers like this are
            listed as a canonical example of when field names would be
            ambiguous without the \_name suffix and should therefore
            include it.
    """

    kms_key_name = proto.Field(proto.STRING, number=2)


class EncryptionInfo(proto.Message):
    r"""Encryption information for a given resource.
    If this resource is protected with customer managed encryption,
    the in-use Cloud KMS key versions will be specified along with
    their status. CMEK is not currently available to end users.

    Attributes:
        encryption_type (google.cloud.spanner_admin_database_v1.types.EncryptionInfo.Type):
            Output only. The type of encryption used to
            protect this resource.
        encryption_status (google.rpc.status_pb2.Status):
            Output only. If present, the status of a
            recent encrypt/decrypt calls on underlying data
            for this resource. Regardless of status, data is
            always encrypted at rest.
        kms_key_version (str):
            Output only. The Cloud KMS key versions used
            for a CMEK-protected Spanner resource.
    """

    class Type(proto.Enum):
        r"""Possible encryption types for a resource."""
        TYPE_UNSPECIFIED = 0
        GOOGLE_DEFAULT_ENCRYPTION = 1
        CUSTOMER_MANAGED_ENCRYPTION = 2

    encryption_type = proto.Field(proto.ENUM, number=3, enum=Type,)

    encryption_status = proto.Field(proto.MESSAGE, number=4, message=status.Status,)

    kms_key_version = proto.Field(proto.STRING, number=2)


__all__ = tuple(sorted(__protobuf__.manifest))
