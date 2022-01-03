# Copyright 2022 Google, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This script wraps the client library in a gRPC interface that a benchmarker
can communicate through."""

from concurrent import futures
from optparse   import OptionParser

import os

import benchmark.benchwrapper.proto.spanner_pb2      as spanner_messages
import benchmark.benchwrapper.proto.spanner_pb2_grpc as spanner_service

from google.cloud import spanner

import grpc

################################## CONSTANTS ##################################

SPANNER_PROJECT  = "someproject"
SPANNER_INSTANCE = "someinstance"
SPANNER_DATABASE = "somedatabase"

###############################################################################


class SpannerBenchWrapperService(spanner_service.SpannerBenchWrapperServicer):
    """Benchwrapper Servicer class to implement Read, Insert and Update
    methods."""

    def __init__(self,
                 project=SPANNER_PROJECT,
                 instance_id=SPANNER_INSTANCE,
                 database_id=SPANNER_DATABASE) -> None:

        spanner_client = spanner.Client(project)
        instance = spanner_client.instance(instance_id)
        self.database = instance.database(database_id)

        super().__init__()

    def Read(self, request, context):
        """Read represents operations like Go's ReadOnlyTransaction.Query,
        Java's ReadOnlyTransaction.executeQuery, Python's snapshot.read, and
        Node's Transaction.Read.

        It will typically be used to read many items.
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(request.query)

        for _ in results:
            # do nothing with the data
            continue

        return spanner_messages.EmptyResponse()

    def Insert(self, request, context):
        """Insert represents operations like Go's Client.Apply, Java's
        DatabaseClient.writeAtLeastOnce, Python's transaction.commit, and Node's
        Transaction.Commit.

        It will typically be used to insert many items.
        """
        with self.database.batch() as batch:
            batch.insert(
                table="Singers",
                columns=("SingerId", "FirstName", "LastName"),
                values=[(i.id, i.first_name, i.last_name) for i in request.singers],
            )

        return spanner_messages.EmptyResponse()

    def Update(self, request, context):
        """Update represents operations like Go's
        ReadWriteTransaction.BatchUpdate, Java's TransactionRunner.run,
        Python's Batch.update, and Node's Transaction.BatchUpdate.

        It will typically be used to update many items.
        """
        self.database.run_in_transaction(self.update_singers, request.queries)

        return spanner_messages.EmptyResponse()

    def update_singers(self, transaction, stmts):
        """Method to execute batch_update in a transaction."""
        status, row_cts = transaction.batch_update(stmts)

        print(status, row_cts)


def get_opts():
    """Parse command line arguments."""
    parser = OptionParser()
    parser.add_option("-p", "--port", help="Specify a port to run on")

    opts, _ = parser.parse_args()

    return opts


def validate_opts(opts):
    """Validate command line arguments."""
    if opts.port is None:
        raise ValueError("Please specify a valid port, e.g., -p 5000 or "
                         "--port 5000.")


def start_grpc_server(num_workers, port):
    """Method to start the GRPC server."""
    # Instantiate the GRPC server.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=num_workers))

    # Instantiate benchwrapper service.
    spanner_benchwrapper_service = SpannerBenchWrapperService()

    # Add benchwrapper servicer to server.
    spanner_service.add_SpannerBenchWrapperServicer_to_server(
        spanner_benchwrapper_service, server)

    # Form the server address.
    addr = "localhost:{0}".format(port)

    # Add the port, and start the server.
    server.add_insecure_port(addr)
    server.start()
    server.wait_for_termination()


def serve():
    """Driver method."""
    opts = get_opts()

    validate_opts(opts)

    if "SPANNER_EMULATOR_HOST" not in os.environ:
        raise ValueError("This benchmarking server only works when connected "
                         "to an emulator. Please set SPANNER_EMULATOR_HOST.")

    start_grpc_server(10, opts.port)


if __name__ == "__main__":
    serve()
  