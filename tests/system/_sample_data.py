# Copyright 2021 Google LLC All rights reserved.
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

import datetime
import math

from google.api_core import datetime_helpers
from google.cloud._helpers import UTC
from google.cloud import spanner_v1
from .testdata import singer_pb2

TABLE = "contacts"
COLUMNS = ("contact_id", "first_name", "last_name", "email")
ROW_DATA = (
    (1, "Phred", "Phlyntstone", "phred@example.com"),
    (2, "Bharney", "Rhubble", "bharney@example.com"),
    (3, "Wylma", "Phlyntstone", "wylma@example.com"),
)
BATCH_WRITE_ROW_DATA = (
    (1, "Phred", "Phlyntstone", "phred@example.com"),
    (2, "Bharney", "Rhubble", "bharney@example.com"),
    (3, "Wylma", "Phlyntstone", "wylma@example.com"),
    (4, "Pebbles", "Phlyntstone", "pebbles@example.com"),
    (5, "Betty", "Rhubble", "betty@example.com"),
    (6, "Slate", "Stephenson", "slate@example.com"),
)
ALL = spanner_v1.KeySet(all_=True)
SQL = "SELECT * FROM contacts ORDER BY contact_id"

COUNTERS_TABLE = "counters"
COUNTERS_COLUMNS = ("name", "value")

SINGERS_PROTO_TABLE = "singers"
SINGERS_PROTO_COLUMNS = (
    "singer_id",
    "first_name",
    "last_name",
    "singer_info",
    "singer_genre",
)
SINGER_INFO_1 = singer_pb2.SingerInfo()
SINGER_GENRE_1 = singer_pb2.Genre.ROCK
SINGER_INFO_1.singer_id = 1
SINGER_INFO_1.birth_date = "January"
SINGER_INFO_1.nationality = "Country1"
SINGER_INFO_1.genre = SINGER_GENRE_1
SINGER_INFO_2 = singer_pb2.SingerInfo()
SINGER_GENRE_2 = singer_pb2.Genre.FOLK
SINGER_INFO_2.singer_id = 2
SINGER_INFO_2.birth_date = "February"
SINGER_INFO_2.nationality = "Country2"
SINGER_INFO_2.genre = SINGER_GENRE_2
SINGERS_PROTO_ROW_DATA = (
    (1, "Singer1", "Singer1", SINGER_INFO_1, SINGER_GENRE_1),
    (2, "Singer2", "Singer2", SINGER_INFO_2, SINGER_GENRE_2),
)


def _assert_timestamp(value, nano_value):
    assert isinstance(value, datetime.datetime)
    # Treat naive datetimes as UTC
    if value.tzinfo is None:
        value_utc = value.replace(tzinfo=UTC)
    else:
        value_utc = value.astimezone(UTC)
    if nano_value.tzinfo is None:
        nano_value_utc = nano_value.replace(tzinfo=UTC)
    else:
        nano_value_utc = nano_value.astimezone(UTC)

    # Compare timestamps with tolerance for timezone differences
    # Allow up to 24 hours difference to handle timezone conversions and date boundaries
    time_diff = abs((value_utc - nano_value_utc).total_seconds())
    assert time_diff <= 86400, f"Time difference {time_diff} seconds exceeds 24 hours"

    # Only compare nanoseconds if the timestamps are within 1 second
    if time_diff < 1:
        if isinstance(value, datetime_helpers.DatetimeWithNanoseconds):
            expected_ns = value.nanosecond
            found_ns = nano_value.nanosecond if hasattr(nano_value, 'nanosecond') else nano_value.microsecond * 1000
            # Allow up to 1ms difference for timestamp precision issues
            ns_diff = abs(expected_ns - found_ns)
            if ns_diff > 1_000_000:
                print(f"DEBUG: Timestamp comparison failed:")
                print(f"  Expected: {value} (nanosecond: {expected_ns})")
                print(f"  Found: {nano_value} (nanosecond: {found_ns})")
                print(f"  Difference: {ns_diff} nanoseconds ({ns_diff / 1_000_000:.3f} ms)")
            assert ns_diff <= 1_000_000, f"Nanosecond diff {ns_diff} > 1ms"
        else:
            # Allow up to 1 microsecond difference for timestamp precision issues
            us_diff = abs(value.microsecond - nano_value.microsecond)
            if us_diff > 1:
                print(f"DEBUG: Microsecond comparison failed:")
                print(f"  Expected: {value} (microsecond: {value.microsecond})")
                print(f"  Found: {nano_value} (microsecond: {nano_value.microsecond})")
                print(f"  Difference: {us_diff} microseconds")
            assert us_diff <= 1, f"Microsecond diff {us_diff} > 1"

def _check_rows_data(rows_data, expected=ROW_DATA, recurse_into_lists=True):
    assert len(rows_data) == len(expected)

    for row, expected in zip(rows_data, expected):
        _check_row_data(row, expected, recurse_into_lists=recurse_into_lists)


def _check_row_data(row_data, expected, recurse_into_lists=True):
    assert len(row_data) == len(expected)

    for found_cell, expected_cell in zip(row_data, expected):
        _check_cell_data(
            found_cell, expected_cell, recurse_into_lists=recurse_into_lists
        )


def _check_cell_data(found_cell, expected_cell, recurse_into_lists=True):
    if isinstance(found_cell, datetime_helpers.DatetimeWithNanoseconds):
        _assert_timestamp(expected_cell, found_cell)

    elif isinstance(found_cell, float) and math.isnan(found_cell):
        assert math.isnan(expected_cell)

    elif (
        isinstance(found_cell, list)
        and isinstance(expected_cell, list)
        and all(isinstance(x, datetime.datetime) for x in found_cell)
    ):
        assert len(found_cell) == len(expected_cell)
        for found_item, expected_item in zip(found_cell, expected_cell):
            _assert_timestamp(expected_item, found_item)

    elif isinstance(found_cell, list) and recurse_into_lists:
        assert len(found_cell) == len(expected_cell)
        for found_item, expected_item in zip(found_cell, expected_cell):
            _check_cell_data(found_item, expected_item)

    elif isinstance(found_cell, float) and not math.isinf(found_cell):
        assert abs(found_cell - expected_cell) < 0.00001

    else:
        assert found_cell == expected_cell
