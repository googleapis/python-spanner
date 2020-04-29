# -*- coding: utf-8 -*-
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Wrappers for protocol buffer enum types."""

import enum


class NullValue(enum.IntEnum):
    """
    Protocol Buffers - Google's data interchange format Copyright 2008
    Google Inc. All rights reserved.
    https://developers.google.com/protocol-buffers/

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are
    met:

    ::

        * Redistributions of source code must retain the above copyright

    notice, this list of conditions and the following disclaimer. \*
    Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution. \*
    Neither the name of Google Inc. nor the names of its contributors may be
    used to endorse or promote products derived from this software without
    specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
    IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
    PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
    OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
    PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    Attributes:
      NULL_VALUE (int): Null value.
    """

    NULL_VALUE = 0


class TypeCode(enum.IntEnum):
    """
    A list of key ranges. See ``KeyRange`` for more information about
    key range specifications.

    Attributes:
      TYPE_CODE_UNSPECIFIED (int): Not specified.
      BOOL (int): OAuth scopes needed for the client.

      Example:

      | service Foo { option (google.api.oauth_scopes) =
      | "https://www.googleapis.com/auth/cloud-platform"; ... }

      If there is more than one scope, use a comma-separated string:

      Example:

      | service Foo { option (google.api.oauth_scopes) =
      | "https://www.googleapis.com/auth/cloud-platform,"
        "https://www.googleapis.com/auth/monitoring"; ... }
      INT64 (int): If present, results will be restricted to the specified partition
      previously created using PartitionQuery(). There must be an exact match
      for the values of fields common to this message and the
      PartitionQueryRequest message used to create this partition_token.
      FLOAT64 (int): Read data at a timestamp >= ``NOW - max_staleness`` seconds.
      Guarantees that all writes that have committed more than the specified
      number of seconds ago are visible. Because Cloud Spanner chooses the
      exact timestamp, this mode works even if the client's local clock is
      substantially skewed from Cloud Spanner commit timestamps.

      Useful for reading the freshest data available at a nearby replica,
      while bounding the possible staleness if the local replica has fallen
      behind.

      Note that this option can only be used in single-use transactions.
      TIMESTAMP (int): Gets a session. Returns ``NOT_FOUND`` if the session does not exist.
      This is mainly useful for determining whether a session is still alive.
      DATE (int): For convenience ``all`` can be set to ``true`` to indicate that this
      ``KeySet`` matches all keys in the table or index. Note that any keys
      specified in ``keys`` or ``ranges`` are only yielded once.
      STRING (int): An option to control the selection of optimizer version.

      This parameter allows individual queries to pick different query
      optimizer versions.

      Specifying "latest" as a value instructs Cloud Spanner to use the latest
      supported query optimizer version. If not specified, Cloud Spanner uses
      optimizer version set at the database level options. Any other positive
      integer (from the list of supported optimizer versions) overrides the
      default optimizer version for query execution. The list of supported
      optimizer versions can be queried from
      SPANNER_SYS.SUPPORTED_OPTIMIZER_VERSIONS. Executing a SQL statement with
      an invalid optimizer version will fail with a syntax error
      (``INVALID_ARGUMENT``) status.

      The ``optimizer_version`` statement hint has precedence over this
      setting.
      BYTES (int): Executes all reads at the given timestamp. Unlike other modes, reads
      at a specific timestamp are repeatable; the same read at the same
      timestamp always returns the same data. If the timestamp is in the
      future, the read will block until the specified timestamp, modulo the
      read's deadline.

      Useful for large scale consistent reads such as mapreduces, or for
      coordinating many reads against a consistent snapshot of the data.

      A timestamp in RFC3339 UTC "Zulu" format, accurate to nanoseconds.
      Example: ``"2014-10-02T15:01:23.045123456Z"``.
      ARRAY (int): The values to be written. ``values`` can contain more than one list
      of values. If it does, then multiple rows are written, one for each
      entry in ``values``. Each list in ``values`` must have exactly as many
      entries as there are entries in ``columns`` above. Sending multiple
      lists is equivalent to sending multiple ``Mutation``\ s, each containing
      one ``values`` entry and repeating ``table`` and ``columns``. Individual
      values in each list are encoded as described ``here``.
      STRUCT (int): The name of the field. For reads, this is the column name. For SQL
      queries, it is the column alias (e.g., ``"Word"`` in the query
      ``"SELECT 'hello' AS Word"``), or the column name (e.g., ``"ColName"``
      in the query ``"SELECT ColName FROM Table"``). Some columns might have
      an empty name (e.g., !"SELECT UPPER(ColName)"`). Note that a query
      result can contain multiple fields with the same name.
    """

    TYPE_CODE_UNSPECIFIED = 0
    BOOL = 1
    INT64 = 2
    FLOAT64 = 3
    TIMESTAMP = 4
    DATE = 5
    STRING = 6
    BYTES = 7
    ARRAY = 8
    STRUCT = 9


class ExecuteSqlRequest(object):
    class QueryMode(enum.IntEnum):
        """
        Mode in which the statement must be processed.

        Attributes:
          NORMAL (int): The default mode. Only the statement results are returned.
          PLAN (int): This mode returns only the query plan, without any results or
          execution statistics information.
          PROFILE (int): This mode returns both the query plan and the execution statistics along
          with the results.
        """

        NORMAL = 0
        PLAN = 1
        PROFILE = 2


class PlanNode(object):
    class Kind(enum.IntEnum):
        """
        If set true, then the Java code generator will generate a separate
        .java file for each top-level message, enum, and service defined in the
        .proto file. Thus, these types will *not* be nested inside the outer
        class named by java_outer_classname. However, the outer class will still
        be generated to contain the file's getDescriptor() method as well as any
        top-level extensions defined in the file.

        Attributes:
          KIND_UNSPECIFIED (int): Not specified.
          RELATIONAL (int): Aggregated statistics from the execution of the query. Only present
          when the query is profiled. For example, a query could return the
          statistics as follows:

          ::

              {
                "rows_returned": "3",
                "elapsed_time": "1.22 secs",
                "cpu_time": "1.19 secs"
              }
          SCALAR (int): Denotes a Scalar node in the expression tree. Scalar nodes represent
          non-iterable entities in the query plan. For example, constants or
          arithmetic operators appearing inside predicate expressions or references
          to column names.
        """

        KIND_UNSPECIFIED = 0
        RELATIONAL = 1
        SCALAR = 2
