# Copyright 2024 Google LLC All rights reserved.
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

import json
from google.cloud.spanner_v1.data_types import JsonObject


class Test_JsonObject_serde(unittest.TestCase):
    def test_w_dict(self):
        data = {"foo": "bar"}
        expected = json.dumps(data, sort_keys=True, separators=(",", ":"))
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_list_of_dict(self):
        data = [{"foo1": "bar1"}, {"foo2": "bar2"}]
        expected = json.dumps(data, sort_keys=True, separators=(",", ":"))
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_JsonObject_of_dict(self):
        data = {"foo": "bar"}
        expected = json.dumps(data, sort_keys=True, separators=(",", ":"))
        data_jsonobject = JsonObject(JsonObject(data))
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_JsonObject_of_list_of_dict(self):
        data = [{"foo1": "bar1"}, {"foo2": "bar2"}]
        expected = json.dumps(data, sort_keys=True, separators=(",", ":"))
        data_jsonobject = JsonObject(JsonObject(data))
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_simple_float_JsonData(self):
        data = 1.1
        expected = json.dumps(data)
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_simple_str_JsonData(self):
        data = "foo"
        expected = json.dumps(data)
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_empty_str_JsonData(self):
        data = ""
        expected = json.dumps(data)
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_None_JsonData(self):
        data = None
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), None)

    def test_w_list_of_simple_JsonData(self):
        data = [1.1, "foo"]
        expected = json.dumps(data, sort_keys=True, separators=(",", ":"))
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_empty_list(self):
        data = []
        expected = json.dumps(data)
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_empty_dict(self):
        data = [{}]
        expected = json.dumps(data)
        data_jsonobject = JsonObject(data)
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_JsonObject_of_simple_JsonData(self):
        data = 1.1
        expected = json.dumps(data)
        data_jsonobject = JsonObject(JsonObject(data))
        self.assertEqual(data_jsonobject.serialize(), expected)

    def test_w_JsonObject_of_list_of_simple_JsonData(self):
        data = [1.1, "foo"]
        expected = json.dumps(data, sort_keys=True, separators=(",", ":"))
        data_jsonobject = JsonObject(JsonObject(data))
        self.assertEqual(data_jsonobject.serialize(), expected)


class Test_JsonObject_serialize_default(unittest.TestCase):
    """Tests for the ``default`` parameter of ``JsonObject.serialize()``."""

    def test_dict_with_custom_type_and_default(self):
        from datetime import datetime

        dt = datetime(2023, 6, 15, 9, 30, 0)
        data = {"ts": dt, "name": "test"}
        obj = JsonObject(data)
        result = obj.serialize(default=lambda o: o.isoformat() if isinstance(o, datetime) else str(o))
        parsed = json.loads(result)
        self.assertEqual(parsed["ts"], "2023-06-15T09:30:00")
        self.assertEqual(parsed["name"], "test")

    def test_array_with_custom_type_and_default(self):
        from datetime import datetime

        dt = datetime(2023, 1, 1)
        data = [dt, "hello"]
        obj = JsonObject(data)
        result = obj.serialize(default=lambda o: o.isoformat() if isinstance(o, datetime) else str(o))
        parsed = json.loads(result)
        self.assertEqual(parsed[0], "2023-01-01T00:00:00")
        self.assertEqual(parsed[1], "hello")

    def test_without_default_raises_on_custom_type(self):
        from datetime import datetime

        data = {"ts": datetime(2023, 1, 1)}
        obj = JsonObject(data)
        with self.assertRaises(TypeError):
            obj.serialize()

    def test_default_none_preserves_existing_behavior(self):
        data = {"foo": "bar"}
        expected = json.dumps(data, sort_keys=True, separators=(",", ":"))
        obj = JsonObject(data)
        self.assertEqual(obj.serialize(default=None), expected)

    def test_scalar_with_default(self):
        from datetime import datetime

        dt = datetime(2023, 6, 15)
        obj = JsonObject(dt)
        result = obj.serialize(default=lambda o: o.isoformat() if isinstance(o, datetime) else str(o))
        self.assertEqual(json.loads(result), "2023-06-15T00:00:00")
