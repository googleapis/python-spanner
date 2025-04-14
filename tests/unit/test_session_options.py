# Copyright 2025 Google LLC All rights reserved.
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
import os
from unittest import TestCase
from google.cloud.spanner_v1.session_options import SessionOptions


class TestSessionOptions(TestCase):
    @classmethod
    def setUpClass(cls):
        cls._original_env = dict(os.environ)

    @classmethod
    def tearDownClass(cls):
        os.environ.clear()
        os.environ.update(cls._original_env)

    def test_use_multiplexed_for_read_only(self):
        session_options = SessionOptions()

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "false"
        self.assertFalse(session_options.use_multiplexed_for_read_only())

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "true"
        self.assertFalse(session_options.use_multiplexed_for_read_only())

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"
        self.assertTrue(session_options.use_multiplexed_for_read_only())

        session_options.disable_multiplexed_for_read_only()
        self.assertFalse(session_options.use_multiplexed_for_read_only())

    def test_use_multiplexed_for_partitioned(self):
        session_options = SessionOptions()

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "false"
        self.assertFalse(session_options.use_multiplexed_for_partitioned())

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED] = "false"
        self.assertFalse(session_options.use_multiplexed_for_partitioned())

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED] = "true"
        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "true"
        self.assertFalse(session_options.use_multiplexed_for_partitioned())

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"
        self.assertTrue(session_options.use_multiplexed_for_partitioned())

        session_options.disable_multiplexed_for_partitioned()
        self.assertFalse(session_options.use_multiplexed_for_partitioned())

    def test_use_multiplexed_for_read_write(self):
        session_options = SessionOptions()

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "false"
        self.assertFalse(session_options.use_multiplexed_for_read_write())

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE] = "false"
        self.assertFalse(session_options.use_multiplexed_for_read_write())

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE] = "true"
        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "true"
        self.assertFalse(session_options.use_multiplexed_for_read_write())

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"
        self.assertTrue(session_options.use_multiplexed_for_read_write())

        session_options.disable_multiplexed_for_read_write()
        self.assertFalse(session_options.use_multiplexed_for_read_write())

    def test_supported_env_var_values(self):
        session_options = SessionOptions()

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"

        true_values = ["1", " 1", " 1", "true", "True", "TRUE", " true "]
        for value in true_values:
            os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = value
            self.assertTrue(session_options.use_multiplexed_for_read_only())

        false_values = ["", "0", "false", "False", "FALSE"]
        for value in false_values:
            os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = value
            self.assertFalse(session_options.use_multiplexed_for_read_only())

        del os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED]
        self.assertFalse(session_options.use_multiplexed_for_read_only())
