# Copyright 2020 Google Inc. All Rights Reserved.
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
import uuid

from google.api_core.exceptions import DeadlineExceeded
from google.cloud import spanner
import pytest
from test_utils.retry import RetryErrors

import version_retention_period

def unique_instance_id():
  """ Creates a unique id for the database. """
  return f"test_instance_{uuid.uuid4().hex[:10]}"


def unique_database_id():
  """ Creates a unique id for the database. """
  return f"test_db_{uuid.uuid4().hex[:10]}"


INSTANCE_ID = unique_instance_id()
DATABASE_ID = unique_database_id()

@pytest.fixture(scope="module")
def spanner_instance():
  spanner_client = spanner.Client()
  instance_config = "{}/instanceConfigs/{}".format(
      spanner_client.project_name, "regional-us-central1"
  )
  instance = spanner_client.instance(INSTANCE_ID, instance_config)
  op = instance.create()
  op.result(120)  # block until completion
  yield instance
  instance.delete()

@RetryErrors(exception=DeadlineExceeded, max_tries=2)
def test_restore_database(capsys):
  retention_period = "7d"
  version_retention_period.create_database_with_version_retention_period(INSTANCE_ID, DATABASE_ID, retention_period)
  out, _ = capsys.readouterr()
  assert (DATABASE_ID + " created with ") in out
  assert ("retention period " + version_retention_period) in out
