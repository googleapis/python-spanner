# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
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

from google.cloud.spanner_v1.types import keys
from google.cloud.spanner_v1.types import mutation
from google.cloud.spanner_v1.types import result_set
from google.cloud.spanner_v1.types import transaction as gs_transaction
from google.cloud.spanner_v1.types import type as gs_type
from google.protobuf import struct_pb2  # type: ignore
from google.protobuf import timestamp_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.spanner.v1",
    manifest={
        "CreateSessionRequest",
        "BatchCreateSessionsRequest",
        "BatchCreateSessionsResponse",
        "Session",
        "GetSessionRequest",
        "ListSessionsRequest",
        "ListSessionsResponse",
        "DeleteSessionRequest",
        "RequestOptions",
        "ExecuteSqlRequest",
        "ExecuteBatchDmlRequest",
        "ExecuteBatchDmlResponse",
        "PartitionOptions",
        "PartitionQueryRequest",
        "PartitionReadRequest",
        "Partition",
        "PartitionResponse",
        "ReadRequest",
        "BeginTransactionRequest",
        "CommitRequest",
        "RollbackRequest",
    },
)


from .requests import (
    CreateSessionRequest,
    BatchCreateSessionsRequest,
    GetSessionRequest,
    ListSessionsRequest,
    DeleteSessionRequest,
    ExecuteSqlRequest,
    ExecuteBatchDmlRequest,
    PartitionQueryRequest,
    PartitionReadRequest,
    ReadRequest,
    BeginTransactionRequest,
    CommitRequest,
    RollbackRequest,
)

from .responses import (
    BatchCreateSessionsResponse,
    ListSessionsResponse,
    ExecuteBatchDmlResponse,
    PartitionResponse,
)


class Session(proto.Message):
    r"""A session in the Cloud Spanner API.

    Attributes:
        name (str):
            Output only. The name of the session. This is
            always system-assigned.
        labels (Mapping[str, str]):
            The labels for the session.

            -  Label keys must be between 1 and 63 characters long and
               must conform to the following regular expression:
               ``[a-z]([-a-z0-9]*[a-z0-9])?``.
            -  Label values must be between 0 and 63 characters long and
               must conform to the regular expression
               ``([a-z]([-a-z0-9]*[a-z0-9])?)?``.
            -  No more than 64 labels can be associated with a given
               session.

            See https://goo.gl/xmQnxf for more information on and
            examples of labels.
        create_time (google.protobuf.timestamp_pb2.Timestamp):
            Output only. The timestamp when the session
            is created.
        approximate_last_use_time (google.protobuf.timestamp_pb2.Timestamp):
            Output only. The approximate timestamp when
            the session is last used. It is typically
            earlier than the actual last use time.
        creator_role (str):
            The database role which created this session.
    """

    name = proto.Field(
        proto.STRING,
        number=1,
    )
    labels = proto.MapField(
        proto.STRING,
        proto.STRING,
        number=2,
    )
    create_time = proto.Field(
        proto.MESSAGE,
        number=3,
        message=timestamp_pb2.Timestamp,
    )
    approximate_last_use_time = proto.Field(
        proto.MESSAGE,
        number=4,
        message=timestamp_pb2.Timestamp,
    )
    creator_role = proto.Field(
        proto.STRING,
        number=5,
    )


class RequestOptions(proto.Message):
    r"""Common request options for various APIs.

    Attributes:
        priority (google.cloud.spanner_v1.types.RequestOptions.Priority):
            Priority for the request.
        request_tag (str):
            A per-request tag which can be applied to queries or reads,
            used for statistics collection. Both request_tag and
            transaction_tag can be specified for a read or query that
            belongs to a transaction. This field is ignored for requests
            where it's not applicable (e.g. CommitRequest). Legal
            characters for ``request_tag`` values are all printable
            characters (ASCII 32 - 126) and the length of a request_tag
            is limited to 50 characters. Values that exceed this limit
            are truncated. Any leading underscore (_) characters will be
            removed from the string.
        transaction_tag (str):
            A tag used for statistics collection about this transaction.
            Both request_tag and transaction_tag can be specified for a
            read or query that belongs to a transaction. The value of
            transaction_tag should be the same for all requests
            belonging to the same transaction. If this request doesn't
            belong to any transaction, transaction_tag will be ignored.
            Legal characters for ``transaction_tag`` values are all
            printable characters (ASCII 32 - 126) and the length of a
            transaction_tag is limited to 50 characters. Values that
            exceed this limit are truncated. Any leading underscore (_)
            characters will be removed from the string.
    """

    class Priority(proto.Enum):
        r"""The relative priority for requests. Note that priority is not
        applicable for
        [BeginTransaction][google.spanner.v1.Spanner.BeginTransaction].

        The priority acts as a hint to the Cloud Spanner scheduler and does
        not guarantee priority or order of execution. For example:

        -  Some parts of a write operation always execute at
           ``PRIORITY_HIGH``, regardless of the specified priority. This may
           cause you to see an increase in high priority workload even when
           executing a low priority request. This can also potentially cause
           a priority inversion where a lower priority request will be
           fulfilled ahead of a higher priority request.
        -  If a transaction contains multiple operations with different
           priorities, Cloud Spanner does not guarantee to process the
           higher priority operations first. There may be other constraints
           to satisfy, such as order of operations.
        """
        PRIORITY_UNSPECIFIED = 0
        PRIORITY_LOW = 1
        PRIORITY_MEDIUM = 2
        PRIORITY_HIGH = 3

    priority = proto.Field(
        proto.ENUM,
        number=1,
        enum=Priority,
    )
    request_tag = proto.Field(
        proto.STRING,
        number=2,
    )
    transaction_tag = proto.Field(
        proto.STRING,
        number=3,
    )


class PartitionOptions(proto.Message):
    r"""Options for a PartitionQueryRequest and
    PartitionReadRequest.

    Attributes:
        partition_size_bytes (int):
            **Note:** This hint is currently ignored by PartitionQuery
            and PartitionRead requests.

            The desired data size for each partition generated. The
            default for this option is currently 1 GiB. This is only a
            hint. The actual size of each partition may be smaller or
            larger than this size request.
        max_partitions (int):
            **Note:** This hint is currently ignored by PartitionQuery
            and PartitionRead requests.

            The desired maximum number of partitions to return. For
            example, this may be set to the number of workers available.
            The default for this option is currently 10,000. The maximum
            value is currently 200,000. This is only a hint. The actual
            number of partitions returned may be smaller or larger than
            this maximum count request.
    """

    partition_size_bytes = proto.Field(
        proto.INT64,
        number=1,
    )
    max_partitions = proto.Field(
        proto.INT64,
        number=2,
    )


class Partition(proto.Message):
    r"""Information returned for each partition returned in a
    PartitionResponse.

    Attributes:
        partition_token (bytes):
            This token can be passed to Read,
            StreamingRead, ExecuteSql, or
            ExecuteStreamingSql requests to restrict the
            results to those identified by this partition
            token.
    """

    partition_token = proto.Field(
        proto.BYTES,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
