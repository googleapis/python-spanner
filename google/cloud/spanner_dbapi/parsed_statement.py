# Copyright 2023 Google LLC All rights reserved.
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
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict

from google.cloud.spanner_dbapi.checksum import ResultsChecksum


class StatementType(Enum):
    CLIENT_SIDE = 1
    DDL = 2
    QUERY = 3
    UPDATE = 4
    INSERT = 5


class ClientSideStatementType(Enum):
    COMMIT = 1
    BEGIN = 2
    ROLLBACK = 3
    SHOW_COMMIT_TIMESTAMP = 4
    SHOW_READ_TIMESTAMP = 5
    START_BATCH_DML = 6
    RUN_BATCH = 7
    ABORT_BATCH = 8
    PARTITION_QUERY = 9
    RUN_PARTITION = 10


class ClientSideStatementParamKey(Enum):
    PARTITIONED_SQL_QUERY = 1
    PARTITION_ID = 2


@dataclass
class Statement:
    sql: str
    params: Any = None
    param_types: Any = None
    checksum: ResultsChecksum = None

    def get_tuple(self):
        return self.sql, self.params, self.param_types


@dataclass
class ParsedStatement:
    statement_type: StatementType
    statement: Statement
    client_side_statement_type: ClientSideStatementType = None
    client_side_statement_params: Dict[ClientSideStatementParamKey, Any] = None
