# Copyright 2016 Google LLC All rights reserved.
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


from dataclasses import fields
from google.protobuf.struct_pb2 import Struct
from google.cloud.spanner_v1 import (
    PartialResultSet,
    ResultSetMetadata,
    ResultSetStats,
    ResultSet,
    RequestOptions,
    Type,
    TypeCode,
    ExecuteSqlRequest,
    ReadRequest,
    StructType,
    TransactionOptions,
    TransactionSelector,
    ExecuteBatchDmlRequest,
    ExecuteBatchDmlResponse,
    param_types
)
from google.cloud.spanner_v1.types import transaction as transaction_type
from google.cloud.spanner_v1.keyset import KeySet

from google.cloud.spanner_v1._helpers import (
    _make_value_pb,
    _merge_query_options,
)

import mock

from google.api_core.retry import Retry
from google.api_core import gapic_v1

from tests._helpers import OpenTelemetryBase, StatusCode

TABLE_NAME = "citizens"
COLUMNS = ["email", "first_name", "last_name", "age"]
VALUES = [
    ["phred@exammple.com", "Phred", "Phlyntstone", 32],
    ["bharney@example.com", "Bharney", "Rhubble", 31],
]
DML_QUERY = """\
INSERT INTO citizens(first_name, last_name, age)
VALUES ("Phred", "Phlyntstone", 32)
"""
DML_QUERY_WITH_PARAM = """
INSERT INTO citizens(first_name, last_name, age)
VALUES ("Phred", "Phlyntstone", @age)
"""
SQL_QUERY = """\
SELECT first_name, last_name, age FROM citizens ORDER BY age"""
SQL_QUERY_WITH_PARAM = """
SELECT first_name, last_name, email FROM citizens WHERE age <= @max_age"""
PARAMS = {"age": 30}
PARAM_TYPES = {"age": Type(code=TypeCode.INT64)}


class TestTransaction(OpenTelemetryBase):

    PROJECT_ID = "project-id"
    INSTANCE_ID = "instance-id"
    INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID
    DATABASE_ID = "database-id"
    DATABASE_NAME = INSTANCE_NAME + "/databases/" + DATABASE_ID
    SESSION_ID = "session-id"
    SESSION_NAME = DATABASE_NAME + "/sessions/" + SESSION_ID
    TRANSACTION_ID = b"DEADBEEF"
    TRANSACTION_TAG = "transaction-tag"

    BASE_ATTRIBUTES = {
        "db.type": "spanner",
        "db.url": "spanner.googleapis.com",
        "db.instance": "testing",
        "net.host.name": "spanner.googleapis.com",
    }

    def _getTargetClass(self):
        from google.cloud.spanner_v1.transaction import Transaction

        return Transaction

    def _make_one(self, session, *args, **kwargs):
        transaction = self._getTargetClass()(session, *args, **kwargs)
        session._transaction = transaction
        return transaction

    def _make_spanner_api(self):
        from google.cloud.spanner_v1 import SpannerClient

        return mock.create_autospec(SpannerClient, instance=True)
    
    def _execute_update_helper(
        self,
        transaction,
        database,
        count=0,
        query_options=None,
        request_options=None,
        retry=gapic_v1.method.DEFAULT,
        timeout=gapic_v1.method.DEFAULT,
        begin=True
    ):
        MODE = 2  # PROFILE
        stats_pb = ResultSetStats(row_count_exact=1)

        api = database.spanner_api = self._make_spanner_api()

        if begin is True:
            transaction_pb = transaction_type.Transaction(
                id = self.TRANSACTION_ID
            )
            metadata_pb = ResultSetMetadata(transaction=transaction_pb)
            api.execute_sql.return_value = ResultSet(stats=stats_pb, metadata=metadata_pb)
        else:
            api.execute_sql.return_value = ResultSet(stats=stats_pb)
        
        transaction.transaction_tag = self.TRANSACTION_TAG
        transaction._execute_sql_count = count

        if request_options is None:
            request_options = RequestOptions()
        elif type(request_options) == dict:
            request_options = RequestOptions(request_options)

        row_count = transaction.execute_update(
            DML_QUERY_WITH_PARAM,
            PARAMS,
            PARAM_TYPES,
            query_mode=MODE,
            query_options=query_options,
            request_options=request_options,
            retry=retry,
            timeout=timeout,
        )

        self.assertEqual(row_count, count + 1)

        if begin is True:
            expected_transaction = TransactionSelector(begin=TransactionOptions(read_write=TransactionOptions.ReadWrite()))
        else:
            expected_transaction = TransactionSelector(id=self.TRANSACTION_ID)

        expected_params = Struct(
            fields={key: _make_value_pb(value) for (key, value) in PARAMS.items()}
        )

        expected_query_options = database._instance._client._query_options
        if query_options:
            expected_query_options = _merge_query_options(
                expected_query_options, query_options
            )
        expected_request_options = request_options
        expected_request_options.transaction_tag = self.TRANSACTION_TAG

        expected_request = ExecuteSqlRequest(
            session=self.SESSION_NAME,
            sql=DML_QUERY_WITH_PARAM,
            transaction=expected_transaction,
            params=expected_params,
            param_types=PARAM_TYPES,
            query_mode=MODE,
            query_options=expected_query_options,
            request_options=request_options,
            seqno=count,
        )
        api.execute_sql.assert_called_once_with(
            request=expected_request,
            retry=retry,
            timeout=timeout,
            metadata=[("google-cloud-resource-prefix", database.name)],
        )

        self.assertEqual(transaction._execute_sql_count, count + 1)

    def _execute_sql_helper(
        self,
        transaction,
        database,
        count=0,
        partition=None,
        sql_count=0,
        query_options=None,
        request_options=None,
        timeout=gapic_v1.method.DEFAULT,
        retry=gapic_v1.method.DEFAULT,
        begin=True
    ):
       

        VALUES = [["bharney", "rhubbyl", 31], ["phred", "phlyntstone", 32]]
        VALUE_PBS = [[_make_value_pb(item) for item in row] for row in VALUES]
        MODE = 2  # PROFILE
        struct_type_pb = StructType(
            fields=[
                StructType.Field(name="first_name", type_=Type(code=TypeCode.STRING)),
                StructType.Field(name="last_name", type_=Type(code=TypeCode.STRING)),
                StructType.Field(name="age", type_=Type(code=TypeCode.INT64)),
            ]
        )
        if begin is True:
            transaction_pb = transaction_type.Transaction(
                id = self.TRANSACTION_ID
            )
            metadata_pb = ResultSetMetadata(row_type=struct_type_pb,transaction=transaction_pb)
        else:
            metadata_pb = ResultSetMetadata(row_type=struct_type_pb)
        stats_pb = ResultSetStats(
            query_stats=Struct(fields={"rows_returned": _make_value_pb(2)})
        )
        result_sets = [
            PartialResultSet(metadata=metadata_pb),
            PartialResultSet(stats=stats_pb),
        ]
        for i in range(len(result_sets)):
            result_sets[i].values.extend(VALUE_PBS[i])
        iterator = _MockIterator(*result_sets)
        api = database.spanner_api = self._make_spanner_api()
        api.execute_streaming_sql.return_value = iterator
        transaction._execute_sql_count = sql_count
        transaction._read_request_count = count
        
        if request_options is None:
            request_options = RequestOptions()
        elif type(request_options) == dict:
            request_options = RequestOptions(request_options)

        result_set = transaction.execute_sql(
            SQL_QUERY_WITH_PARAM,
            PARAMS,
            PARAM_TYPES,
            query_mode=MODE,
            query_options=query_options,
            request_options=request_options,
            partition=partition,
            retry=retry,
            timeout=timeout,
        )

        self.assertEqual(transaction._read_request_count, count + 1)

        self.assertEqual(list(result_set), VALUES)
        self.assertEqual(result_set.metadata, metadata_pb)
        self.assertEqual(result_set.stats, stats_pb)

        if begin is True:
            expected_transaction = TransactionSelector(begin=TransactionOptions(read_write=TransactionOptions.ReadWrite()))
        else:
            expected_transaction = TransactionSelector(id=self.TRANSACTION_ID)

        expected_params = Struct(
            fields={key: _make_value_pb(value) for (key, value) in PARAMS.items()}
        )

        expected_query_options = database._instance._client._query_options
        if query_options:
            expected_query_options = _merge_query_options(
                expected_query_options, query_options
            )
        expected_request_options = request_options

        expected_request = ExecuteSqlRequest(
            session=self.SESSION_NAME,
            sql=SQL_QUERY_WITH_PARAM,
            transaction=expected_transaction,
            params=expected_params,
            param_types=PARAM_TYPES,
            query_mode=MODE,
            query_options=expected_query_options,
            request_options=expected_request_options,
            partition_token=partition,
            seqno=sql_count,
        )
        api.execute_streaming_sql.assert_called_once_with(
            request=expected_request,
            metadata=[("google-cloud-resource-prefix", database.name)],
            timeout=timeout,
            retry=retry,
        )

        self.assertEqual(transaction._execute_sql_count, sql_count + 1)

    def _read_helper(
        self,
        transaction,
        database,
        count=0,
        partition=None,
        timeout=gapic_v1.method.DEFAULT,
        retry=gapic_v1.method.DEFAULT,
        request_options=None,
        begin=True
    ):
        VALUES = [["bharney", 31], ["phred", 32]]
        VALUE_PBS = [[_make_value_pb(item) for item in row] for row in VALUES]
        struct_type_pb = StructType(
            fields=[
                StructType.Field(name="name", type_=Type(code=TypeCode.STRING)),
                StructType.Field(name="age", type_=Type(code=TypeCode.INT64)),
            ]
        )

        if begin is True:
            transaction_pb = transaction_type.Transaction(
                id = self.TRANSACTION_ID
            )
            metadata_pb = ResultSetMetadata(row_type=struct_type_pb,transaction=transaction_pb)
        else:
            metadata_pb = ResultSetMetadata(row_type=struct_type_pb)
        
        stats_pb = ResultSetStats(
            query_stats=Struct(fields={"rows_returned": _make_value_pb(2)})
        )
        result_sets = [
            PartialResultSet(metadata=metadata_pb),
            PartialResultSet(stats=stats_pb),
        ]
        for i in range(len(result_sets)):
            result_sets[i].values.extend(VALUE_PBS[i])
        KEYS = [["bharney@example.com"], ["phred@example.com"]]
        keyset = KeySet(keys=KEYS)
        INDEX = "email-address-index"
        LIMIT = 20
        api = database.spanner_api = self._make_spanner_api()
        api.streaming_read.return_value = _MockIterator(*result_sets)
        transaction._read_request_count = count
        
        if request_options is None:
            request_options = RequestOptions()
        elif type(request_options) == dict:
            request_options = RequestOptions(request_options)
        if partition is not None:  # 'limit' and 'partition' incompatible
            result_set = transaction.read(
                TABLE_NAME,
                COLUMNS,
                keyset,
                index=INDEX,
                partition=partition,
                retry=retry,
                timeout=timeout,
                request_options=request_options,
            )
        else:
            result_set = transaction.read(
                TABLE_NAME,
                COLUMNS,
                keyset,
                index=INDEX,
                limit=LIMIT,
                retry=retry,
                timeout=timeout,
                request_options=request_options,
            )

        self.assertEqual(transaction._read_request_count, count + 1)

        self.assertIs(result_set._source, transaction)

        self.assertEqual(list(result_set), VALUES)
        self.assertEqual(result_set.metadata, metadata_pb)
        self.assertEqual(result_set.stats, stats_pb)

        if begin is True:
            expected_transaction = TransactionSelector(begin=TransactionOptions(read_write=TransactionOptions.ReadWrite()))
        else:
            expected_transaction = TransactionSelector(id=self.TRANSACTION_ID)

        if partition is not None:
            expected_limit = 0
        else:
            expected_limit = LIMIT

        # Transaction tag is ignored for read request.
        expected_request_options = request_options
        expected_request_options.transaction_tag = None

        expected_request = ReadRequest(
            session=self.SESSION_NAME,
            table=TABLE_NAME,
            columns=COLUMNS,
            key_set=keyset._to_pb(),
            transaction=expected_transaction,
            index=INDEX,
            limit=expected_limit,
            partition_token=partition,
            request_options=expected_request_options,
        )
        api.streaming_read.assert_called_once_with(
            request=expected_request,
            metadata=[("google-cloud-resource-prefix", database.name)],
            retry=retry,
            timeout=timeout,
        )

    def _batch_update_helper(self, transaction, database, error_after=None, count=0, request_options=None, begin=True):
        from google.rpc.status_pb2 import Status
        insert_dml = "INSERT INTO table(pkey, desc) VALUES (%pkey, %desc)"
        insert_params = {"pkey": 12345, "desc": "DESCRIPTION"}
        insert_param_types = {"pkey": param_types.INT64, "desc": param_types.STRING}
        update_dml = 'UPDATE table SET desc = desc + "-amended"'
        delete_dml = "DELETE FROM table WHERE desc IS NULL"

        dml_statements = [
            (insert_dml, insert_params, insert_param_types),
            update_dml,
            delete_dml,
        ]

        stats_pbs = [
            ResultSetStats(row_count_exact=1),
            ResultSetStats(row_count_exact=2),
            ResultSetStats(row_count_exact=3),
        ]
        if error_after is not None:
            stats_pbs = stats_pbs[:error_after]
            expected_status = Status(code=400)
        else:
            expected_status = Status(code=200)
        expected_row_counts = [stats.row_count_exact for stats in stats_pbs]
        if begin is True:
            transaction_pb = transaction_type.Transaction(
                id = self.TRANSACTION_ID
            )
            metadata_pb = ResultSetMetadata(transaction=transaction_pb)
            result_sets_pb = [ResultSet(stats=stats_pb, metadata= metadata_pb) for stats_pb in stats_pbs]

        else:
            result_sets_pb = [ResultSet(stats=stats_pb) for stats_pb in stats_pbs]

        response = ExecuteBatchDmlResponse(
            status=expected_status,
            result_sets=result_sets_pb,
        )

        api = database.spanner_api = self._make_spanner_api()
        api.execute_batch_dml.return_value = response
        transaction.transaction_tag = self.TRANSACTION_TAG
        transaction._execute_sql_count = count

        if request_options is None:
            request_options = RequestOptions()
        elif type(request_options) == dict:
            request_options = RequestOptions(request_options)

        status, row_counts = transaction.batch_update(
            dml_statements, request_options=request_options
        )

        self.assertEqual(status, expected_status)
        self.assertEqual(row_counts, expected_row_counts)

        if begin is True:
            expected_transaction = TransactionSelector(begin=TransactionOptions(read_write=TransactionOptions.ReadWrite()))
        else:
            expected_transaction = TransactionSelector(id=self.TRANSACTION_ID)
        
        expected_insert_params = Struct(
            fields={
                key: _make_value_pb(value) for (key, value) in insert_params.items()
            }
        )
        expected_statements = [
            ExecuteBatchDmlRequest.Statement(
                sql=insert_dml,
                params=expected_insert_params,
                param_types=insert_param_types,
            ),
            ExecuteBatchDmlRequest.Statement(sql=update_dml),
            ExecuteBatchDmlRequest.Statement(sql=delete_dml),
        ]
        expected_request_options = request_options
        expected_request_options.transaction_tag = self.TRANSACTION_TAG

        expected_request = ExecuteBatchDmlRequest(
            session=self.SESSION_NAME,
            transaction=expected_transaction,
            statements=expected_statements,
            seqno=count,
            request_options=expected_request_options,
        )
        api.execute_batch_dml.assert_called_once_with(
            request=expected_request,
            metadata=[("google-cloud-resource-prefix", database.name)],
        )

        self.assertEqual(transaction._execute_sql_count, count + 1)

    def test_insert(self, transaction):
        from google.cloud.spanner_v1 import Mutation

        transaction.insert(TABLE_NAME, columns=COLUMNS, values=VALUES)

        self.assertEqual(len(transaction._mutations), 1)
        mutation = transaction._mutations[0]
        self.assertIsInstance(mutation, Mutation)
        write = mutation.insert
        self.assertIsInstance(write, Mutation.Write)
        self.assertEqual(write.table, TABLE_NAME)
        self.assertEqual(write.columns, COLUMNS)
        self._compare_values(write.values, VALUES)

    def test_transaction_should_include_begin_with_first_update(self):
        database = _Database()
        session = _Session(database)
        transaction = self._make_one(session)
        self._execute_update_helper(transaction=transaction, database=database)

    def test_transaction_should_include_begin_with_first_query(self):
        database = _Database()
        session = _Session(database)
        transaction = self._make_one(session)
        self._execute_sql_helper(transaction=transaction, database=database)

    def test_transaction_should_include_begin_with_first_read(self):
        database = _Database()
        session = _Session(database)
        transaction = self._make_one(session)
        self._read_helper(transaction=transaction, database=database)

    def test_transaction_should_include_begin_with_first_batch_update(self):
        database = _Database()
        session = _Session(database)
        transaction = self._make_one(session)
        self._batch_update_helper(transaction=transaction, database=database)

    def test_transaction_should_use_transaction_id_returned_by_first_query(self):
        database = _Database()
        session = _Session(database)
        transaction = self._make_one(session)
        self._execute_sql_helper(transaction=transaction, database=database)
        self._execute_update_helper(transaction=transaction, database=database, begin=False)

    def test_transaction_should_use_transaction_id_returned_by_first_update(self):
        database = _Database()
        session = _Session(database)
        transaction = self._make_one(session)
        self._execute_update_helper(transaction=transaction, database=database, begin=True)
        self._execute_sql_helper(transaction=transaction, database=database, begin=False)

    def test_transaction_should_use_transaction_id_returned_by_first_read(self):
        database = _Database()
        session = _Session(database)
        transaction = self._make_one(session)
        self._read_helper(transaction=transaction, database=database, begin=True)
        self._batch_update_helper(transaction=transaction, database=database, begin=False)
        
    def test_transaction_should_use_transaction_id_returned_by_first_batch_update(self):
        database = _Database()
        session = _Session(database)
        transaction = self._make_one(session)
        self._batch_update_helper(transaction=transaction, database=database, begin=True)
        self._read_helper(transaction=transaction, database=database, begin=False)

class _Client(object):
    def __init__(self):
        from google.cloud.spanner_v1 import ExecuteSqlRequest

        self._query_options = ExecuteSqlRequest.QueryOptions(optimizer_version="1")


class _Instance(object):
    def __init__(self):
        self._client = _Client()


class _Database(object):
    def __init__(self):
        self.name = "testing"
        self._instance = _Instance()


class _Session(object):

    _transaction = None

    def __init__(self, database=None, name=TestTransaction.SESSION_NAME):
        self._database = database
        self.name = name

class _MockIterator(object):
    def __init__(self, *values, **kw):
        self._iter_values = iter(values)
        self._fail_after = kw.pop("fail_after", False)
        self._error = kw.pop("error", Exception)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._iter_values)
        except StopIteration:
            if self._fail_after:
                raise self._error
            raise

    next = __next__
