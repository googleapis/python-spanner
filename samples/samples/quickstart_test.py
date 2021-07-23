# Copyright 2016 Google Inc. All Rights Reserved.
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

import pytest

import quickstart


@pytest.fixture(scope="module")
def sample_name():
    return "quickstart"


def test_quickstart(capsys, instance_id, sample_database):
    quickstart.run_quickstart(instance_id, sample_database.database_id)
    out, _ = capsys.readouterr()

    assert "[1]" in out
