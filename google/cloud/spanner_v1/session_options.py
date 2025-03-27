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

from google.cloud.spanner_v1._opentelemetry_tracing import (
    get_current_span,
    add_span_event,
)


class SessionOptions(object):
    """Represents the session options for the Cloud Spanner Python client.

    We can use ::class::`SessionOptions` to determine whether multiplexed sessions should be used for:
    * read-only transactions (:meth:`use_multiplexed_for_read_only`)
    * partitioned transactions (:meth:`use_multiplexed_for_partitioned`)
    * read/write transactions (:meth:`use_multiplexed_for_read_write`).

    The use of multiplexed session can be disabled for corresponding transaction types by calling:
    * :meth:`disable_multiplexed_for_read_only`
    * :meth:`disable_multiplexed_for_partitioned`
    * :meth:`disable_multiplexed_for_read_write`.
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
        self._is_multiplexed_enabled_for_read_only = True
        self._is_multiplexed_enabled_for_partitioned = True
        self._is_multiplexed_enabled_for_read_write = True

    def use_multiplexed_for_read_only(self) -> bool:
        """Returns whether to use multiplexed sessions for read-only transactions.
        Multiplexed sessions are enabled for read-only transactions if:
        * ENV_VAR_ENABLE_MULTIPLEXED is set to true;
        * ENV_VAR_FORCE_DISABLE_MULTIPLEXED is not set to true; and
        * multiplexed sessions have not been disabled for read-only transactions (see 'disable_multiplexed_for_read_only').
        """

        return (
            self._is_multiplexed_enabled_for_read_only
            and self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED)
            and not self._getenv(self.ENV_VAR_FORCE_DISABLE_MULTIPLEXED)
        )

    def disable_multiplexed_for_read_only(self) -> None:
        """Disables the use of multiplexed sessions for read-only transactions."""

        current_span = get_current_span()
        add_span_event(
            current_span,
            "Disabling use of multiplexed session for read-only transactions",
        )

        self._is_multiplexed_enabled_for_read_only = False

    def use_multiplexed_for_partitioned(self) -> bool:
        """Returns whether to use multiplexed sessions for partitioned transactions.
        Multiplexed sessions are enabled for partitioned transactions if:
        * ENV_VAR_ENABLE_MULTIPLEXED is set to true;
        * ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED is set to true;
        * ENV_VAR_FORCE_DISABLE_MULTIPLEXED is not set to true; and
        * multiplexed sessions have not been disabled for partitioned transactions (see 'disable_multiplexed_for_partitioned').
        """

        return (
            self._is_multiplexed_enabled_for_partitioned
            and self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED)
            and self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED)
            and not self._getenv(self.ENV_VAR_FORCE_DISABLE_MULTIPLEXED)
        )

    def disable_multiplexed_for_partitioned(self) -> None:
        """Disables the use of multiplexed sessions for read-only transactions."""

        current_span = get_current_span()
        add_span_event(
            current_span,
            "Disabling use of multiplexed session for partitioned transactions",
        )

        self._is_multiplexed_enabled_for_partitioned = False

    def use_multiplexed_for_read_write(self) -> bool:
        """Returns whether to use multiplexed sessions for read/write transactions.
        Multiplexed sessions are enabled for read/write transactions if:
        * ENV_VAR_ENABLE_MULTIPLEXED is set to true;
        * ENV_VAR_ENABLE_MULTIPLEXED_FOR_PARTITIONED is set to true;
        * ENV_VAR_FORCE_DISABLE_MULTIPLEXED is not set to true; and
        * multiplexed sessions have not been disabled for read/write transactions (see 'disable_multiplexed_for_read_write').
        """

        return (
            self._is_multiplexed_enabled_for_read_write
            and self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED)
            and self._getenv(self.ENV_VAR_ENABLE_MULTIPLEXED_FOR_READ_WRITE)
            and not self._getenv(self.ENV_VAR_FORCE_DISABLE_MULTIPLEXED)
        )

    def disable_multiplexed_for_read_write(self) -> None:
        """Disables the use of multiplexed sessions for read/write transactions."""

        current_span = get_current_span()
        add_span_event(
            current_span,
            "Disabling use of multiplexed session for read/write transactions",
        )

        self._is_multiplexed_enabled_for_read_write = False

    @staticmethod
    def _getenv(name: str) -> bool:
        """Returns the value of the given environment variable as a boolean.
        True values are '1' and 'true' (case-insensitive); all other values are considered false.
        """
        env_var = os.getenv(name, "").lower().strip()
        return env_var in ["1", "true"]
