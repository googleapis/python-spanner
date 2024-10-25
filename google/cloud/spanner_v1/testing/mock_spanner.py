# Copyright 2024 Google LLC All rights reserved.
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

from google.protobuf import empty_pb2  # type: ignore
from google.protobuf import struct_pb2  # type: ignore
import google.cloud.spanner_v1.testing.spanner_pb2_grpc as spanner_grpc
import google.cloud.spanner_v1.types.result_set as result_set
import google.cloud.spanner_v1.types.transaction as transaction
import google.cloud.spanner_v1.types.commit_response as commit
import google.cloud.spanner_v1.types.spanner as spanner
from concurrent import futures
import grpc


class MockSpanner:
    def __init__(self):
        self.results = {}

    def add_result(self, sql: str, result: result_set.ResultSet):
        self.results[sql] = result

    def get_result_as_partial_result_sets(
        self, sql: str
    ) -> [result_set.PartialResultSet]:
        result: result_set.ResultSet = self.results.get(sql)
        if result is None:
            return []
        partials = []
        first = True
        for row in result.rows:
            partial = result_set.PartialResultSet()
            if first:
                partial.metadata = result.metadata
            partial.values.extend(row)
            partials.append(partial)
        return partials


# An in-memory mock Spanner server that can be used for testing.
class SpannerServicer(spanner_grpc.SpannerServicer):
    def __init__(self):
        self._requests = []
        self.session_counter = 0
        self.sessions = {}
        self._mock_spanner = MockSpanner()

    @property
    def mock_spanner(self):
        return self._mock_spanner

    @property
    def requests(self):
        return self._requests

    def CreateSession(self, request, context):
        self._requests.append(request)
        return self.__create_session(request.database, request.session)

    def BatchCreateSessions(self, request, context):
        self._requests.append(request)
        sessions = []
        for i in range(request.session_count):
            sessions.append(
                self.__create_session(request.database, request.session_template)
            )
        return spanner.BatchCreateSessionsResponse(dict(session=sessions))

    def __create_session(self, database: str, session_template: spanner.Session):
        self.session_counter += 1
        session = spanner.Session()
        session.name = database + "/sessions/" + str(self.session_counter)
        session.multiplexed = session_template.multiplexed
        session.labels.MergeFrom(session_template.labels)
        session.creator_role = session_template.creator_role
        self.sessions[session.name] = session
        return session

    def GetSession(self, request, context):
        return spanner.Session()

    def ListSessions(self, request, context):
        return [spanner.Session()]

    def DeleteSession(self, request, context):
        return empty_pb2.Empty()

    def ExecuteSql(self, request, context):
        return result_set.ResultSet()

    def ExecuteStreamingSql(self, request, context):
        self._requests.append(request)
        partials = self.mock_spanner.get_result_as_partial_result_sets(request.sql)
        for result in partials:
            yield result

    def ExecuteBatchDml(self, request, context):
        return spanner.ExecuteBatchDmlResponse()

    def Read(self, request, context):
        return result_set.ResultSet()

    def StreamingRead(self, request, context):
        for result in [result_set.PartialResultSet(), result_set.PartialResultSet()]:
            yield result

    def BeginTransaction(self, request, context):
        return transaction.Transaction()

    def Commit(self, request, context):
        return commit.CommitResponse()

    def Rollback(self, request, context):
        return empty_pb2.Empty()

    def PartitionQuery(self, request, context):
        return spanner.PartitionResponse()

    def PartitionRead(self, request, context):
        return spanner.PartitionResponse()

    def BatchWrite(self, request, context):
        for result in [spanner.BatchWriteResponse(), spanner.BatchWriteResponse()]:
            yield result


def start_mock_server() -> (grpc.Server, SpannerServicer, int):
    spanner_servicer = SpannerServicer()
    spanner_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    spanner_grpc.add_SpannerServicer_to_server(spanner_servicer, spanner_server)
    port = spanner_server.add_insecure_port("[::]:0")
    spanner_server.start()
    return spanner_server, spanner_servicer, port


if __name__ == "__main__":
    server, _ = start_mock_server()
    server.wait_for_termination()
