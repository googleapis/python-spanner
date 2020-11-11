# Copyright 2020 Google LLC All rights reserved.
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

import unittest


class TestDBAPIGlobals(unittest.TestCase):
    def test_apilevel(self):
        from google.cloud.spanner_dbapi import apilevel
        from google.cloud.spanner_dbapi import paramstyle
        from google.cloud.spanner_dbapi import threadsafety

        self.assertEqual(apilevel, "2.0", "We implement PEP-0249 version 2.0")
        self.assertEqual(paramstyle, "format", "Cloud Spanner uses @param")
        self.assertEqual(
            threadsafety, 1, "Threads may share module but not connections"
        )
