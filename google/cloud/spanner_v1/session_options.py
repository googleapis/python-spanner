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
import logging
import os
from enum import Enum


class TransactionType(Enum):
    """Transaction types for session options."""

    READ_ONLY = "read-only"
    PARTITIONED = "partitioned"
    READ_WRITE = "read/write"


class SessionOptions(object):
    """Represents the session options for the Cloud Spanner Python client.
    We can use :class:`SessionOptions` to determine whether multiplexed sessions
    should be used for a specific transaction type with :meth:`use_multiplexed`. The use
    of multiplexed session can be disabled for a specific transaction type or for all
    transaction types with :meth:`disable_multiplexed`.
    """

    # Environment variables for multiplexed sessions
    ENV_VAR_ENABLE_MULTIPLEXED = "GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS"
    ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED = (
        "GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_PARTITIONED_OPS"
    )
    ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE = (
        "GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW"
    )
    ENV_VAR_FORCE_DISABLE_MULTIPLEXED = (
        "GOOGLE_CLOUD_SPANNER_FORCE_DISABLE_MULTIPLEXED_SESSIONS"
    )

    def __init__(self):
        # Internal overrides to disable the use of multiplexed
        # sessions in case of runtime errors.
        self._is_multiplexed_enabled = {
            TransactionType.READ_ONLY: True,
            TransactionType.PARTITIONED: True,
            TransactionType.READ_WRITE: True,
        }

    def use_multiplexed(self, transaction_type: TransactionType) -> bool:
        """Returns whether to use multiplexed sessions for the given transaction type.

        Multiplexed sessions are enabled for read-only transactions if:
            * ENV_VAR_ENABLE_MULTIPLEXED is set to true;
            * ENV_VAR_FORCE_DISABLE_MULTIPLEXED is not set to true; and
            * multiplexed sessions have not been disabled for read-only transactions.

        Multiplexed sessions are enabled for partitioned transactions if:
            * ENV_VAR_ENABLE_MULTIPLEXED is set to true;
            * ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED is set to true;
            * ENV_VAR_FORCE_DISABLE_MULTIPLEXED is not set to true; and
            * multiplexed sessions have not been disabled for partitioned transactions.

        Multiplexed sessions are enabled for read/write transactions if:
            * ENV_VAR_ENABLE_MULTIPLEXED is set to true;
            * ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE is set to true;
            * ENV_VAR_FORCE_DISABLE_MULTIPLEXED is not set to true; and
            * multiplexed sessions have not been disabled for read/write transactions.

        :type transaction_type: :class:`TransactionType`
        :param transaction_type: the type of transaction
        """

        if transaction_type is TransactionType.READ_ONLY:
            return (
                self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED)
                and not self._getenv(self.ENV_VAR_FORCE_DISABLE_MULTIPLEXED)
                and self._is_multiplexed_enabled[transaction_type]
            )

        elif transaction_type is TransactionType.PARTITIONED:
            return (
                self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED)
                and self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED)
                and not self._getenv(self.ENV_VAR_FORCE_DISABLE_MULTIPLEXED)
                and self._is_multiplexed_enabled[transaction_type]
            )

        elif transaction_type is TransactionType.READ_WRITE:
            return (
                self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED)
                and self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE)
                and not self._getenv(self.ENV_VAR_FORCE_DISABLE_MULTIPLEXED)
                and self._is_multiplexed_enabled[transaction_type]
            )

        raise ValueError(f"Transaction type {transaction_type} is not supported.")

    def disable_multiplexed(
        self, logger: logging.Logger = None, transaction_type: TransactionType = None
    ) -> None:
        """Disables the use of multiplexed sessions for the given transaction type.
        If no transaction type is specified, disables the use of multiplexed sessions
        for all transaction types.

        :type logger: :class:`Logger`
        :param logger: logger for logging disabling the use of multiplexed sessions.

        :type transaction_type: :class:`TransactionType`
        :param transaction_type: (Optional) the type of transaction for which to disable
            the use of multiplexed sessions.
        """

        if transaction_type and transaction_type not in self._is_multiplexed_enabled:
            raise ValueError(f"Transaction type '{transaction_type}' is not supported.")

        logger = logger or logging.getLogger(__name__)

        transaction_types_to_disable = (
            [transaction_type]
            if transaction_type is not None
            else list(TransactionType)
        )

        for transaction_type_to_disable in transaction_types_to_disable:
            logger.warning(
                f"Disabling multiplexed sessions for {transaction_type_to_disable.value} transactions"
            )
            self._is_multiplexed_enabled[transaction_type_to_disable] = False

        return

    @staticmethod
    def _getenv(name: str) -> bool:
        """Returns the value of the given environment variable as a boolean.
        True values are '1' and 'true' (case-insensitive); all other values are
        considered false.
        """
        env_var = os.getenv(name, "").lower().strip()
        return env_var in ["1", "true"]
