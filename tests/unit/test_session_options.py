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
from logging import Logger
from unittest import TestCase

from google.cloud.spanner_v1.session_options import SessionOptions, TransactionType


class TestSessionOptions(TestCase):
    _logger = Logger("test_session_options_logger")

    @classmethod
    def setUpClass(cls):
        # Save the original environment variables.
        cls._original_env = dict(os.environ)

    @classmethod
    def tearDownClass(cls):
        # Restore environment variables.
        os.environ.clear()
        os.environ.update(cls._original_env)

    def test_use_multiplexed_for_read_only(self):
        session_options = SessionOptions()
        transaction_type = TransactionType.READ_ONLY

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "false"
        self.assertFalse(session_options.use_multiplexed(transaction_type))

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "true"
        self.assertFalse(session_options.use_multiplexed(transaction_type))

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"
        self.assertTrue(session_options.use_multiplexed(transaction_type))

        session_options.disable_multiplexed(self._logger, transaction_type)
        self.assertFalse(session_options.use_multiplexed(transaction_type))

    def test_use_multiplexed_for_partitioned(self):
        session_options = SessionOptions()
        transaction_type = TransactionType.PARTITIONED

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "false"
        self.assertFalse(session_options.use_multiplexed(transaction_type))

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED] = "false"
        self.assertFalse(session_options.use_multiplexed(transaction_type))

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED] = "true"
        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "true"
        self.assertFalse(session_options.use_multiplexed(transaction_type))

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"
        self.assertTrue(session_options.use_multiplexed(transaction_type))

        session_options.disable_multiplexed(self._logger, transaction_type)
        self.assertFalse(session_options.use_multiplexed(transaction_type))

    def test_use_multiplexed_for_read_write(self):
        session_options = SessionOptions()
        transaction_type = TransactionType.READ_WRITE

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "false"
        self.assertFalse(session_options.use_multiplexed(transaction_type))

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE] = "false"
        self.assertFalse(session_options.use_multiplexed(transaction_type))

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE] = "true"
        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "true"
        self.assertFalse(session_options.use_multiplexed(transaction_type))

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"
        self.assertTrue(session_options.use_multiplexed(transaction_type))

        session_options.disable_multiplexed(self._logger, transaction_type)
        self.assertFalse(session_options.use_multiplexed(transaction_type))

    def test_disable_multiplexed_all(self):
        session_options = SessionOptions()

        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = "true"
        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED] = "true"
        os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE] = "true"
        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"

        session_options.disable_multiplexed(self._logger)

        self.assertFalse(session_options.use_multiplexed(TransactionType.READ_ONLY))
        self.assertFalse(session_options.use_multiplexed(TransactionType.PARTITIONED))
        self.assertFalse(session_options.use_multiplexed(TransactionType.READ_WRITE))

    def test_unsupported_transaction_type(self):
        session_options = SessionOptions()
        unsupported_type = "UNSUPPORTED_TRANSACTION_TYPE"

        with self.assertRaises(ValueError):
            session_options.use_multiplexed(unsupported_type)

        with self.assertRaises(ValueError):
            session_options.disable_multiplexed(self._logger, unsupported_type)

    def test_env_var_values(self):
        session_options = SessionOptions()

        os.environ[SessionOptions.ENV_VAR_FORCE_DISABLE_MULTIPLEXED] = "false"

        true_values = ["1", " 1", " 1", "true", "True", "TRUE", " true "]
        for value in true_values:
            os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = value
            self.assertTrue(session_options.use_multiplexed(TransactionType.READ_ONLY))

        false_values = ["", "0", "false", "False", "FALSE"]
        for value in false_values:
            os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED] = value
            self.assertFalse(session_options.use_multiplexed(TransactionType.READ_ONLY))

        del os.environ[SessionOptions.ENV_VAR_ENABLE_MULTIPLEXED]
        self.assertFalse(session_options.use_multiplexed(TransactionType.READ_ONLY))
