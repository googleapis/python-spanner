# Copyright 20203 Google LLC All rights reserved.
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


class StatementType(Enum):
    CLIENT_SIDE = 1
    DDL = 2
    QUERY = 3
    UPDATE = 4
    INSERT = 5


class ClientSideStatementType(Enum):
    COMMIT = 1
    BEGIN = 2


@dataclass
class ParsedStatement:
    statement_type: StatementType
    query: str
    client_side_statement_type: ClientSideStatementType = None
