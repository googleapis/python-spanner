# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import unittest


class Test_compare_checksums(unittest.TestCase):
    def test_equal(self):
        from google.cloud.spanner_dbapi.checksum import _compare_checksums
        from google.cloud.spanner_dbapi.checksum import ResultsChecksum

        original = ResultsChecksum()
        original.consume_result(5)

        retried = ResultsChecksum()
        retried.consume_result(5)

        self.assertIsNone(_compare_checksums(original, retried))

    def test_less_results(self):
        from google.cloud.spanner_dbapi.checksum import _compare_checksums
        from google.cloud.spanner_dbapi.checksum import ResultsChecksum
        from google.cloud.spanner_dbapi.exceptions import RetryAborted

        original = ResultsChecksum()
        original.consume_result(5)

        retried = ResultsChecksum()

        with self.assertRaises(RetryAborted):
            _compare_checksums(original, retried)

    def test_more_results(self):
        from google.cloud.spanner_dbapi.checksum import _compare_checksums
        from google.cloud.spanner_dbapi.checksum import ResultsChecksum
        from google.cloud.spanner_dbapi.exceptions import RetryAborted

        original = ResultsChecksum()
        original.consume_result(5)

        retried = ResultsChecksum()
        retried.consume_result(5)
        retried.consume_result(2)

        with self.assertRaises(RetryAborted):
            _compare_checksums(original, retried)

    def test_mismatch(self):
        from google.cloud.spanner_dbapi.checksum import _compare_checksums
        from google.cloud.spanner_dbapi.checksum import ResultsChecksum
        from google.cloud.spanner_dbapi.exceptions import RetryAborted

        original = ResultsChecksum()
        original.consume_result(5)

        retried = ResultsChecksum()
        retried.consume_result(2)

        with self.assertRaises(RetryAborted):
            _compare_checksums(original, retried)
