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


class Instance(object):
    class State(enum.IntEnum):
        """
        Indicates the current state of the instance.

        Attributes:
          STATE_UNSPECIFIED (int): Not specified.
          CREATING (int): The instance is still being created. Resources may not be
          available yet, and operations such as database creation may not
          work.
          READY (int): The instance is fully created and ready to do work such as
          creating databases.
        """

        STATE_UNSPECIFIED = 0
        CREATING = 1
        READY = 2


class ReplicaInfo(object):
    class ReplicaType(enum.IntEnum):
        """
        Signed fractions of a second at nanosecond resolution of the span of
        time. Durations less than one second are represented with a 0
        ``seconds`` field and a positive or negative ``nanos`` field. For
        durations of one second or more, a non-zero value for the ``nanos``
        field must be of the same sign as the ``seconds`` field. Must be from
        -999,999,999 to +999,999,999 inclusive.

        Attributes:
          TYPE_UNSPECIFIED (int): Not specified.
          READ_WRITE (int): Indicates the type of replica. See the `replica types
          documentation <https://cloud.google.com/spanner/docs/replication#replica_types>`__
          for more details.
          READ_ONLY (int): A Timestamp represents a point in time independent of any time zone
          or local calendar, encoded as a count of seconds and fractions of
          seconds at nanosecond resolution. The count is relative to an epoch at
          UTC midnight on January 1, 1970, in the proleptic Gregorian calendar
          which extends the Gregorian calendar backwards to year one.

          All minutes are 60 seconds long. Leap seconds are "smeared" so that no
          leap second table is needed for interpretation, using a `24-hour linear
          smear <https://developers.google.com/time/smear>`__.

          The range is from 0001-01-01T00:00:00Z to
          9999-12-31T23:59:59.999999999Z. By restricting to that range, we ensure
          that we can convert to and from `RFC
          3339 <https://www.ietf.org/rfc/rfc3339.txt>`__ date strings.

          # Examples

          Example 1: Compute Timestamp from POSIX ``time()``.

          ::

              Timestamp timestamp;
              timestamp.set_seconds(time(NULL));
              timestamp.set_nanos(0);

          Example 2: Compute Timestamp from POSIX ``gettimeofday()``.

          ::

              struct timeval tv;
              gettimeofday(&tv, NULL);

              Timestamp timestamp;
              timestamp.set_seconds(tv.tv_sec);
              timestamp.set_nanos(tv.tv_usec * 1000);

          Example 3: Compute Timestamp from Win32 ``GetSystemTimeAsFileTime()``.

          ::

              FILETIME ft;
              GetSystemTimeAsFileTime(&ft);
              UINT64 ticks = (((UINT64)ft.dwHighDateTime) << 32) | ft.dwLowDateTime;

              // A Windows tick is 100 nanoseconds. Windows epoch 1601-01-01T00:00:00Z
              // is 11644473600 seconds before Unix epoch 1970-01-01T00:00:00Z.
              Timestamp timestamp;
              timestamp.set_seconds((INT64) ((ticks / 10000000) - 11644473600LL));
              timestamp.set_nanos((INT32) ((ticks % 10000000) * 100));

          Example 4: Compute Timestamp from Java ``System.currentTimeMillis()``.

          ::

              long millis = System.currentTimeMillis();

              Timestamp timestamp = Timestamp.newBuilder().setSeconds(millis / 1000)
                  .setNanos((int) ((millis % 1000) * 1000000)).build();

          Example 5: Compute Timestamp from current time in Python.

          ::

              timestamp = Timestamp()
              timestamp.GetCurrentTime()

          # JSON Mapping

          In JSON format, the Timestamp type is encoded as a string in the `RFC
          3339 <https://www.ietf.org/rfc/rfc3339.txt>`__ format. That is, the
          format is "{year}-{month}-{day}T{hour}:{min}:{sec}[.{frac_sec}]Z" where
          {year} is always expressed using four digits while {month}, {day},
          {hour}, {min}, and {sec} are zero-padded to two digits each. The
          fractional seconds, which can go up to 9 digits (i.e. up to 1 nanosecond
          resolution), are optional. The "Z" suffix indicates the timezone
          ("UTC"); the timezone is required. A proto3 JSON serializer should
          always use UTC (as indicated by "Z") when printing the Timestamp type
          and a proto3 JSON parser should be able to accept both UTC and other
          timezones (as indicated by an offset).

          For example, "2017-01-15T01:30:15.01Z" encodes 15.01 seconds past 01:30
          UTC on January 15, 2017.

          In JavaScript, one can convert a Date object to this format using the
          standard
          `toISOString() <https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString>`__
          method. In Python, a standard ``datetime.datetime`` object can be
          converted to this format using
          ```strftime`` <https://docs.python.org/2/library/time.html#time.strftime>`__
          with the time format spec '%Y-%m-%dT%H:%M:%S.%fZ'. Likewise, in Java,
          one can use the Joda Time's
          ```ISODateTimeFormat.dateTime()`` <http://www.joda.org/joda-time/apidocs/org/joda/time/format/ISODateTimeFormat.html#dateTime%2D%2D>`__
          to obtain a formatter capable of generating timestamps in this format.
          WITNESS (int): An annotation that describes a resource definition without a
          corresponding message; see ``ResourceDescriptor``.
        """

        TYPE_UNSPECIFIED = 0
        READ_WRITE = 1
        READ_ONLY = 2
        WITNESS = 3
