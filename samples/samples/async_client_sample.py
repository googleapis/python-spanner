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

"""
This application demonstrates how to do basic asynchronous operations using Cloud Spanner.
"""


import asyncio
from google.cloud.spanner_v1 import (
    keyset,
    transaction,
    Client,
    session
)
from google.cloud.spanner_v1.types import (
    CreateSessionRequest,
    BatchCreateSessionsRequest,
    GetSessionRequest,
    ListSessionsRequest,
    DeleteSessionRequest,
    ExecuteSqlRequest,
    ExecuteBatchDmlRequest,
    ReadRequest,
    TransactionOptions,
    BeginTransactionRequest,
    CommitRequest,
    RollbackRequest,
    TransactionSelector,
    PartitionQueryRequest,
    PartitionReadRequest
)
from google.cloud.spanner_v1.services import spanner
import tracemalloc
import argparse
import time


# [ FUNCTION for creating async client and spanner client object]
def get_async_client_and_associate_database(instance_id, database_id):
    async_client = spanner.SpannerAsyncClient()
    spanner_client = Client()
    instance = spanner_client.instance(instance_id)
    database_object = instance.database(database_id)
    return async_client, database_object


# [START async_client_partition_read]
async def partition_read(database_object, async_client):

     """Creates a set of partition tokens that can be
     used to execute a read operation in parallel."""

     session_object = session.Session(database_object)
     session_object.create()

     request = PartitionReadRequest(
         session = session_object.name,
         transaction = TransactionSelector(
             begin = TransactionOptions(
                 read_only = TransactionOptions.ReadOnly(
                     strong = True,
                 ),
             ),
         ),
         table = 'Singers',
         key_set = keyset.KeySet(all_=True)._to_pb(),
     )
     response = await async_client.partition_read(request = request)
     return response

# [END async_client_partition_read]


# [START async_client_partition_query]
async def partition_query(database_object, async_client):

     """Creates a set of partition tokens that can be
     used to execute a query operation in parallel."""

     session_object = session.Session(database_object)
     session_object.create()

     request = PartitionQueryRequest(
         session = session_object.name,
         transaction = TransactionSelector(
             begin = TransactionOptions(
                 read_only = TransactionOptions.ReadOnly(
                     strong = True,
                 ),
             ),
         ),
         sql =  "UPDATE Singers SET FirstName = 'William' WHERE SingerId = 1003",
     )
     response = await async_client.partition_query(request = request)
     return response

# [END async_client_partition_query]


# [START async_client_rollback]
async def rollback(database_object, async_client):

     """Rollbacks a transaction, releases any locks it holds."""

     session_object = session.Session(database_object)
     session_object.create()

     transaction_object = transaction.Transaction(session_object)
     transaction_id_blob = transaction_object.begin()

     request = RollbackRequest(
         session = session_object.name,
         transaction_id = transaction_id_blob,
     )
     await async_client.rollback(request = request)

# [END async_client_rollback]


# [START async_client_commit]
async def commit(database_object, async_client):
     
     """Commits a transaction. The request includes the mutations to be
     applied to rows in the database."""

     session_object = session.Session(database_object)
     session_object.create()

     transaction_object = transaction.Transaction(session_object)
     transaction_id_blob = transaction_object.begin()

     request = CommitRequest(
         transaction_id = transaction_id_blob,
         session = session_object.name
     )
     response = await async_client.commit(request = request)
     return response

# [END async_client_commit]


# [START async_client_begin_transaction]
async def begin_transaction(database_object, async_client):

     """Begins a new transaction."""

     session_object = session.Session(database_object)
     session_object.create()

     txn_options = TransactionOptions(
         read_write = TransactionOptions.ReadWrite()
     )

     request = BeginTransactionRequest(
         session = session_object.name,
         options = txn_options,
     )
     response = await async_client.begin_transaction(request = request)
     return response

# [END async_client_begin_transaction]


# [START async_client_streaming_read]
async def streaming_read(database_object, async_client):

     """Like [Read][google.cloud.spanner_v1.services.Spanner.Read],
     except returns the result set as a stream."""

     session_object = session.Session(database_object)
     session_object.create()

     request = ReadRequest(
         session = session_object.name,
         table =  'Singers',
         columns = ["SingerId", "FirstName", "LastName"],
         key_set = keyset.KeySet(all_=True)._to_pb(),
     )
     stream = await async_client.streaming_read(request = request)
     response_list = []
     async for response in stream:
         response_list.append(response)
     return response_list

# [END async_client_streaming_read]


# [START async_client_execute_batch_dml]
async def execute_batch_dml(database_object, async_client):

     """Executes a batch of SQL DML statements."""

     session_object = session.Session(database_object)
     session_object.create()

     statements = ExecuteBatchDmlRequest.Statement(
        sql = (
            "INSERT INTO Singers"
            "(SingerId, FirstName, LastName)"
            "Values (1003, 'Steven', 'Smith')"
        ),
     ),

     request = ExecuteBatchDmlRequest(
         session = session_object.name,
         transaction = TransactionSelector(
             begin = TransactionOptions(
                read_write = TransactionOptions.ReadWrite(),
             ),
         ),
         statements = statements,
         seqno=550,
     )
     response = await async_client.execute_batch_dml(request = request)
     return response

# [END async_client_execute_batch_dml]


# [START async_client_execute_streaming_sql]
async def execute_streaming_sql(database_object, async_client):

     """Like [ExecuteSql][google.cloud.spanner_v1.services.Spanner.ExecuteSql],
     except returns the result set as a stream."""

     session_object = session.Session(database_object)
     session_object.create()

     request = ExecuteSqlRequest(
         session = session_object.name,
         sql =  "SELECT SingerId, FirstName, LastName FROM Singers",
     )
     stream = await async_client.execute_streaming_sql(request = request)
     response_list = []
     async for response in stream:
         response_list.append(response)

# [END async_client_execute_streaming_sql]


# [START async_client_read]
async def read(database_object, async_client):

     """Reads rows from the database using key lookups and scans,
     as a simple key/value style alternative to
     [ExecuteSql][google.cloud.spanner_v1.services.Spanner.ExecuteSql]"""

     session_object = session.Session(database_object)
     session_object.create()

     request = ReadRequest(
         session = session_object.name,
         table =  'Singers',
         columns = ["SingerId", "FirstName", "LastName"],
         key_set = keyset.KeySet(all_=True)._to_pb(),
     )
     response = await async_client.read(request = request)
     return response

# [END async_client_read]


# [START async_client_execute_sql]
async def execute_sql(database_object, async_client):

     """Executes an SQL statement, returns all results in a single
        reply. This method cannot be used to return a result set larger
        than 10 MiB; if the query yields more data than that, the query
        fails with a ``FAILED_PRECONDITION`` error."""

     session_object = session.Session(database_object)
     session_object.create()

     request = ExecuteSqlRequest(
         session = session_object.name,
         sql =  "SELECT SingerId, FirstName, LastName FROM Singers",
     )
     response = await async_client.execute_sql(request = request)
     return response

# [END async_client_execute_sql]


# [START async_client_delete_session]
async def delete_session(session_name, async_client):

     """Ends a session, releasing server resources associated
        with it. This will asynchronously trigger cancellation
        of any operations that are running with this session."""

     request = DeleteSessionRequest(
         name=session_name,
     )
     await async_client.delete_session(request = request)

# [END async client delete_session]


# [START async_client_list_sessions]
async def list_sessions(database_object, async_client):

     """Lists all sessions in a given database."""

     request = ListSessionsRequest(
         database = database_object.name,
     )
     response = await async_client.list_sessions(request = request)
     return response

# [END async_client_get_session]


# [START async_client_get_session]
async def get_session(database_object, async_client):

     """Gets a session. Prints ``NOT_FOUND`` if the session does not
        exist. This is mainly useful for determining whether a session
        is still alive."""

     session_object = session.Session(database_object)
     session_object.create()

     request = GetSessionRequest(
         name = session_object.name,
     )
     response = await async_client.get_session(request = request)
     return response

# [END async_client_get_session]


# [START async_client_batch_create_session]
async def batch_create_sessions(database_object, async_client):

     """Creates multiple new sessions."""
     
     request = BatchCreateSessionsRequest(
         database = database_object.name,
         session_count=1420, # The number of sessions to be created in this batch call
     )
     response = await async_client.batch_create_sessions(request = request)
     return response

# [END async_client_batch_create_session]


# [START async_client_create_session]
async def create_session(database_object, async_client):

     """Creates a new session."""

     request = CreateSessionRequest(
         database = database_object.name,
     )
     response = await async_client.create_session(request = request)
     return response

# [END async_client_create_session]


async def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--instance_id", help="Your Cloud Spanner instance ID."
    )
    parser.add_argument(
        "--database_id", help="Your Cloud Spanner database ID."
    )
    parser.add_argument(
        "--session_name", help="Session name."
    )
    parser.add_argument(
        "--n", help="Number of queries you want to make."
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser(
        "create_session",
        help=create_session.__doc__
    )
    subparsers.add_parser(
        "batch_create_sessions",
        help=batch_create_sessions.__doc__
    )
    subparsers.add_parser(
        "get_session",
        help=get_session.__doc__
    )
    subparsers.add_parser(
        "list_sessions",
        help=list_sessions.__doc__
    )
    subparsers.add_parser(
        "delete_session",
        help=delete_session.__doc__
    )
    subparsers.add_parser(
        "execute_sql",
        help=execute_sql.__doc__
    )
    subparsers.add_parser(
        "execute_streaming_sql",
        help=execute_streaming_sql.__doc__
    )
    subparsers.add_parser(
        "execute_batch_dml",
        help=execute_batch_dml.__doc__
    )
    subparsers.add_parser(
        "read",
        help=read.__doc__
    )
    subparsers.add_parser(
        "streaming_read",
        help=streaming_read.__doc__
    )
    subparsers.add_parser(
        "begin_transaction",
        help=begin_transaction.__doc__
    )
    subparsers.add_parser(
        "commit",
        help=commit.__doc__
    )
    subparsers.add_parser(
        "rollback",
        help=rollback.__doc__
    )
    subparsers.add_parser(
        "partition_query",
        help=partition_query.__doc__
    )
    subparsers.add_parser(
        "partition_read",
        help=partition_read.__doc__
    )
    args = parser.parse_args()

    async_client, database_object = get_async_client_and_associate_database(args.instance_id, args.database_id)

    if args.command == "create_session":
        await asyncio.gather(
            *[create_session(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "batch_create_sessions":
        await asyncio.gather(
            *[batch_create_sessions(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "get_session":
        await asyncio.gather(
            *[get_session(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "list_sessions":
        await asyncio.gather(
            *[list_sessions(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "delete_session":
        await asyncio.gather(
            *[delete_session(args.session_name, async_client) for task in range(int(args.n))]
        )
    elif args.command == "execute_sql":
        await asyncio.gather(
            *[execute_sql(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "execute_streaming_sql":
        await asyncio.gather(
            *[execute_streaming_sql(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "execute_batch_dml":
        await asyncio.gather(
            *[execute_batch_dml(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "read":
        await asyncio.gather(
            *[read(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "streaming_read":
        await asyncio.gather(
            *[streaming_read(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "begin_transaction":
        await asyncio.gather(
            *[begin_transaction(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "commit":
        await asyncio.gather(
            *[commit(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "rollback":
        await asyncio.gather(
            *[rollback(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "partition_query":
        await asyncio.gather(
            *[partition_query(database_object, async_client) for task in range(int(args.n))]
        )
    elif args.command == "partition_read":
        await asyncio.gather(
            *[partition_read(database_object, async_client) for task in range(int(args.n))]
        )
    
    
if __name__ == "__main__":

    startTime = time.time()
    tracemalloc.start()
    asyncio.run(main())
    endTime = time.time()
    print('Total time taken: ' + str(endTime-startTime) + ' seconds')
    print('Total memory taken(current, peak): ' + str(tracemalloc.get_traced_memory()) + ' KiB')
    tracemalloc.stop()