# Copyright 2015 gRPC authors.
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
"""The Python implementation of the GRPC helloworld.Greeter server."""

from concurrent import futures
import logging

import grpc
from google.spanner.executor.v1 import cloud_executor_pb2
from google.spanner.executor.v1 import cloud_executor_pb2_grpc
from google.cloud.spanner import Client


class TestProxyServer(cloud_executor_pb2_grpc.SpannerExecutorProxyServicer):
    def ExecuteActionAsync(self, request, context):
        print("Execute action async")
        print(f"{request=}")
        client = Client()
        # TODO: Replace with correct client code
        client.list_instances()
        # TODO: Do something with the request
        yield cloud_executor_pb2.SpannerAsyncActionResponse()

def serve():
    port = '50055'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cloud_executor_pb2_grpc.add_SpannerExecutorProxyServicer_to_server(TestProxyServer(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
