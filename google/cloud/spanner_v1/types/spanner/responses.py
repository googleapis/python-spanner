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

__manifest__ = (
    "BatchCreateSessionsResponse",
    "ListSessionsResponse",
    "ExecuteBatchDmlResponse",
    "PartitionResponse",
)


class BatchCreateSessionsResponse(proto.Message):
    r"""The response for
    [BatchCreateSessions][google.spanner.v1.Spanner.BatchCreateSessions].

    Attributes:
        session (Sequence[google.cloud.spanner_v1.types.Session]):
            The freshly created sessions.
    """
    __module__ = __module__.rsplit(".", maxsplit=1)[0]  # type: ignore

    session = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="Session",
    )


class ListSessionsResponse(proto.Message):
    r"""The response for
    [ListSessions][google.spanner.v1.Spanner.ListSessions].

    Attributes:
        sessions (Sequence[google.cloud.spanner_v1.types.Session]):
            The list of requested sessions.
        next_page_token (str):
            ``next_page_token`` can be sent in a subsequent
            [ListSessions][google.spanner.v1.Spanner.ListSessions] call
            to fetch more of the matching sessions.
    """
    __module__ = __module__.rsplit(".", maxsplit=1)[0]  # type: ignore

    @property
    def raw_page(self):
        return self

    sessions = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="Session",
    )
    next_page_token = proto.Field(
        proto.STRING,
        number=2,
    )


class ExecuteBatchDmlResponse(proto.Message):
    r"""The response for
    [ExecuteBatchDml][google.spanner.v1.Spanner.ExecuteBatchDml].
    Contains a list of [ResultSet][google.spanner.v1.ResultSet]
    messages, one for each DML statement that has successfully executed,
    in the same order as the statements in the request. If a statement
    fails, the status in the response body identifies the cause of the
    failure.

    To check for DML statements that failed, use the following approach:

    1. Check the status in the response message. The
       [google.rpc.Code][google.rpc.Code] enum value ``OK`` indicates
       that all statements were executed successfully.
    2. If the status was not ``OK``, check the number of result sets in
       the response. If the response contains ``N``
       [ResultSet][google.spanner.v1.ResultSet] messages, then statement
       ``N+1`` in the request failed.

    Example 1:

    -  Request: 5 DML statements, all executed successfully.
    -  Response: 5 [ResultSet][google.spanner.v1.ResultSet] messages,
       with the status ``OK``.

    Example 2:

    -  Request: 5 DML statements. The third statement has a syntax
       error.
    -  Response: 2 [ResultSet][google.spanner.v1.ResultSet] messages,
       and a syntax error (``INVALID_ARGUMENT``) status. The number of
       [ResultSet][google.spanner.v1.ResultSet] messages indicates that
       the third statement failed, and the fourth and fifth statements
       were not executed.

    Attributes:
        result_sets (Sequence[google.cloud.spanner_v1.types.ResultSet]):
            One [ResultSet][google.spanner.v1.ResultSet] for each
            statement in the request that ran successfully, in the same
            order as the statements in the request. Each
            [ResultSet][google.spanner.v1.ResultSet] does not contain
            any rows. The
            [ResultSetStats][google.spanner.v1.ResultSetStats] in each
            [ResultSet][google.spanner.v1.ResultSet] contain the number
            of rows modified by the statement.

            Only the first [ResultSet][google.spanner.v1.ResultSet] in
            the response contains valid
            [ResultSetMetadata][google.spanner.v1.ResultSetMetadata].
        status (google.rpc.status_pb2.Status):
            If all DML statements are executed successfully, the status
            is ``OK``. Otherwise, the error status of the first failed
            statement.
    """
    __module__ = __module__.rsplit(".", maxsplit=1)[0]  # type: ignore

    result_sets = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message=result_set.ResultSet,
    )
    status = proto.Field(
        proto.MESSAGE,
        number=2,
        message=status_pb2.Status,
    )


class PartitionResponse(proto.Message):
    r"""The response for
    [PartitionQuery][google.spanner.v1.Spanner.PartitionQuery] or
    [PartitionRead][google.spanner.v1.Spanner.PartitionRead]

    Attributes:
        partitions (Sequence[google.cloud.spanner_v1.types.Partition]):
            Partitions created by this request.
        transaction (google.cloud.spanner_v1.types.Transaction):
            Transaction created by this request.
    """
    __module__ = __module__.rsplit(".", maxsplit=1)[0]  # type: ignore

    partitions = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="Partition",
    )
    transaction = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gs_transaction.Transaction,
    )


__all__ = tuple(sorted(__manifest__))
