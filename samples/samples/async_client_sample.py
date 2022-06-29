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

from asyncore import readwrite
from curses import meta
from email.message import Message
from importlib.metadata import metadata
from inspect import trace
from google.cloud.spanner_v1.services import spanner
from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1 import session
from numpy import partition
from google.cloud.spanner_v1.types import CreateSessionRequest
from google.cloud.spanner_v1.types import GetSessionRequest
from google.cloud.spanner_v1.types import ListSessionsRequest
from google.cloud.spanner_v1.types import ExecuteSqlRequest
from google.cloud.spanner_v1.types import ReadRequest
from google.cloud.spanner_v1.types import ExecuteBatchDmlRequest
from google.cloud.spanner_v1.types import BeginTransactionRequest
from google.cloud.spanner_v1.types import TransactionOptions
from google.cloud.spanner_v1.types import CommitRequest
from google.cloud.spanner_v1.types import RollbackRequest
from google.cloud.spanner_v1.types import PartitionQueryRequest
from google.cloud.spanner_v1.types import PartitionReadRequest
from google.cloud.spanner_v1.types import TransactionSelector
from google.cloud.spanner_v1.types import BatchCreateSessionsRequest
from google.cloud.spanner_v1.types import DeleteSessionRequest
import asyncio
from google.cloud.spanner_v1 import keyset
from google.cloud.spanner_v1 import transaction
import time
import tracemalloc
import argparse


# [ FUNCTION for creating async client and spanner client object]
def getObjects(instance_id, database_id):
    # Create async client object
    async_client = spanner.SpannerAsyncClient()

    # Creates spanner client object
    spanner_client = Client()

    # Creates the instance object associated with instance id
    instance = spanner_client.instance(instance_id)

    # Creates the database object associated with instance object
    database_object = instance.database(database_id)

    # Returns async client object and database object
    return async_client, database_object

# [START async client partition read]
async def partition_read(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Request object for Partition Read."""
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

     """Execute Partition Read."""
     response = await async_client.partition_read(request = request)

     """Prints the response."""
     print(response)

     print("\nEnding task: ", task_num)
# [END async client partition read]

# [START async client partition query]
async def partition_query(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Request object for Partition Query."""
     request = PartitionQueryRequest(
         session = session_object.name,
         transaction = TransactionSelector(
             begin = TransactionOptions(
                 read_only = TransactionOptions.ReadOnly(
                     strong = True,
                 ),
             ),
         ),
         sql =  "UPDATE Singers SET FirstName = 'Trivedi' WHERE SingerId = 1003",
     )

     """Partition the sql statement query."""
     response = await async_client.partition_query(request = request)

     """Prints the response."""
     print(response)

     print("\nEnding task: ", task_num)
# [END async client partition query]

# [START async client rollback]
async def rollback(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Transaction object associated with the session object"""
     transaction_object = transaction.Transaction(session_object)

     """Begin Transaction"""
     transaction_id_blob = transaction_object.begin()

     """Request object for Rollback."""
     request = RollbackRequest(
         session = session_object.name,
         transaction_id = transaction_id_blob,
     )

     """Rolled back the Transaction."""
     await async_client.rollback(request = request)

     print("\nEnding task: ", task_num)
# [END async client rollback]

# [START async client commit]
async def commit(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Transaction object associated with the session object"""
     transaction_object = transaction.Transaction(session_object)

     """Begin Transaction"""
     transaction_id_blob = transaction_object.begin()

     """Request object for Commit."""
     request = CommitRequest(
         transaction_id = transaction_id_blob,
         session = session_object.name
     )

     """Commit the transaction"""
     response = await async_client.commit(request = request)

     """Prints the commit transaction response."""
     print(response)

     print("\nEnding task: ", task_num)
# [END async client commit]

# [START async client begin transaction]
async def begin_transaction(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Read write transaction object."""
     transactionOption_object = TransactionOptions(
         read_write = TransactionOptions.ReadWrite()
     )

     """Request object for begin transaction."""
     request = BeginTransactionRequest(
         session = session_object.name,
         options = transactionOption_object,
     )

     """Begins Transaction."""
     response = await async_client.begin_transaction(request = request)

     """Prints the response."""
     print(response)

     print("\nEnding task: ", task_num)
     print("\n")
# [END async client begin transaction]

# [START async client streaming read]
async def streaming_read(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Request object for read."""
     request = ReadRequest(
         session = session_object.name,
         table =  'Singers',
         columns = ["SingerId", "FirstName", "LastName"],
         key_set = keyset.KeySet(all_=True)._to_pb(),
     )

     """Executes streaming read and get the response in stream"""
     stream = await async_client.streaming_read(request = request)

     """Prints the stream response."""
     async for response in stream:
         print(response)

     print("\nEnding task: ", task_num)
# [END async client streaming read]

# [START async client execute batch dml]
async def execute_batch_dml(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()
     statements = ExecuteBatchDmlRequest.Statement(
        sql = "INSERT INTO Singers (SingerId, FirstName, LastName) Values (1003, 'Alka', 'Trivedi')",
     ),

     """Request object for execute batch dml."""
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

     """Executes batch dml"""
     response = await async_client.execute_batch_dml(request = request)

     """Prints the response."""
     print(response)

     print("\nEnding task: ", task_num)
# [END async client execute batch dml]

# [START async client execute streaming sql]
async def execute_streaming_sql(database_object, async_client, task_num):

     print("\nstarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Request object for execute sql."""
     request = ExecuteSqlRequest(
         session = session_object.name,
         sql =  "SELECT SingerId, FirstName, LastName FROM Singers",
     )

     """Execute the sql statement and get the response in a stream"""
     stream = await async_client.execute_streaming_sql(request = request)

     """Prints the streaming response."""
     async for response in stream:
         print(response)

     print("\nEnding task: ", task_num)
# [END async client execute streaming sql]

# [START async client read]
async def read(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()


     """Request object for read."""
     request = ReadRequest(
         session = session_object.name,
         table =  'Singers',
         columns = ["SingerId", "FirstName", "LastName"],
         key_set = keyset.KeySet(all_=True)._to_pb(),
     )

     """Reads the columns data from the table"""
     response = await async_client.read(request = request)

     """Prints the response."""
     print(response)

     print("\nEnding task: ", task_num)
# [END async client read]


# [START async client execute_sql]
async def execute_sql(database_object, async_client, task_num):
     print("\nstarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Request object for execute sql."""
     request = ExecuteSqlRequest(
         session = session_object.name,
         sql =  "SELECT SingerId, FirstName, LastName FROM Singers",
     )

     """Execute the sql statement."""
     response = await async_client.execute_sql(request = request)

     """Prints the response."""
     print(response)

     print("\nEnding task: ", task_num)

# [END async client execute_sql]


# [START async client delete_session]
async def delete_session(session_name, async_client, task_num):

     print("\nStarting task: ", task_num)

     """Request object for delete session."""
     request = DeleteSessionRequest(
         name=session_name,
     )

     """Delete the session."""
     await async_client.delete_session(request = request)

     print("\nEnding task: ", task_num)

# [END async client delete_session]

# [START async client list_sessions]
async def list_sessions(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     """Request object for list session."""
     request = ListSessionsRequest(
         database = database_object.name,
     )

     """Get the list of sessions."""
     response = await async_client.list_sessions(request = request)

     """Prints the session."""
     print(response)

     print("\nEnding task: ", task_num)

# [END async client get_session]


# [START async client get_session]
async def get_session(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     # Create the session object associated with the database object
     session_object = session.Session(database_object)

     """Create the session."""
     session_object.create()

     """Request object for get session."""
     request = GetSessionRequest(
         name = session_object.name,
     )

     """Get the session."""
     response = await async_client.get_session(request = request)

     """Prints the session got."""
     print(response)

     print("\nEnding task: ", task_num)

# [END async client get_session]


# [START async client batch_create_session]
async def batch_create_session(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     """Request object for batch create session."""
     request = BatchCreateSessionsRequest(
         database = database_object.name,
         session_count=1420,
     )

     """Creates batch sessions."""
     response = await async_client.batch_create_sessions(request = request)

     """Prints the sessions batchwise."""
     print(response)

     print("\nEnding task: ", task_num)

# [END async client batch_create_session]


# [START async client create_session]
async def create_session(database_object, async_client, task_num):

     print("\nStarting task: ", task_num)

     """Request object for create session."""
     request = CreateSessionRequest(
         database = database_object.name,
     )

     """Creates a session."""
     response = await async_client.create_session(request = request)

     """Prints the session."""
     print(response)

     print("\nEnding task: ", task_num)

# [END async client create_session]


async def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--instance_id", help="Your Cloud Spanner instance ID.")
    parser.add_argument(
        "--database_id", help="Your Cloud Spanner database ID.", default="example_db"
    )
    parser.add_argument(
        "--session_name", help="session name."
    )
    parser.add_argument(
        "--n", help="number of queries."
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser(
        "create_session",
        help=create_session.__doc__
    )
    subparsers.add_parser(
        "batch_create_session",
        help=batch_create_session.__doc__
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

    async_client, database_object = getObjects(args.instance_id, args.database_id)

    if args.command == "create_session":
        await asyncio.gather(
            *[create_session(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "batch_create_session":
        await asyncio.gather(
            *[batch_create_session(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "get_session":
        await asyncio.gather(
            *[get_session(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "list_sessions":
        await asyncio.gather(
            *[list_sessions(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "delete_session":
        await asyncio.gather(
            *[delete_session(args.session_name, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "execute_sql":
        await asyncio.gather(
            *[execute_sql(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "execute_streaming_sql":
        await asyncio.gather(
            *[execute_streaming_sql(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "execute_batch_dml":
        await asyncio.gather(
            *[execute_batch_dml(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "read":
        await asyncio.gather(
            *[read(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "streaming_read":
        await asyncio.gather(
            *[streaming_read(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "begin_transaction":
        await asyncio.gather(
            *[begin_transaction(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "commit":
        await asyncio.gather(
            *[commit(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "rollback":
        await asyncio.gather(
            *[rollback(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "partition_query":
        await asyncio.gather(
            *[partition_query(database_object, async_client, task) for task in range(int(args.n))]
        )
    elif args.command == "partition_read":
        await asyncio.gather(
            *[partition_read(database_object, async_client, task) for task in range(int(args.n))]
        )
    
    
if __name__ == "__main__":
    
    # Assigned start time of the execution to startTime
    startTime = time.time()

    # Started keeping track of memory taken during execution
    tracemalloc.start()

    # Called the main function
    asyncio.run(main())

    # Assigned end time of the execution to endTime
    endTime = time.time()

    # Prints the total execution time
    print('total time taken: ' + str(endTime-startTime) + ' seconds')

    # Prints the total memory taken
    print('total memory taken(current, peak): ', tracemalloc.get_traced_memory())

     # Stopped keeping track of memory taken during execution
    tracemalloc.stop()