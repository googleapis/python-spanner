# Copyright 2021 Google LLC
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

"""Custom data types for spanner."""

import json
from google.protobuf.message import Message
from google.protobuf.internal.enum_type_wrapper import EnumTypeWrapper


class JsonObject(dict):
    """
    Provides functionality of JSON data type in Cloud Spanner
    API, mimicking simple `dict()` behaviour and making
    all the necessary conversions under the hood.
    """

    def __init__(self, *args, **kwargs):
        self._is_null = (args, kwargs) == ((), {}) or args == (None,)
        self._is_array = len(args) and isinstance(args[0], (list, tuple))

        # if the JSON object is represented with an array,
        # the value is contained separately
        if self._is_array:
            self._array_value = args[0]
            return

        if not self._is_null:
            super(JsonObject, self).__init__(*args, **kwargs)

    def __repr__(self):
        if self._is_array:
            return str(self._array_value)

        return super(JsonObject, self).__repr__()

    @classmethod
    def from_str(cls, str_repr):
        """Initiate an object from its `str` representation.

        Args:
            str_repr (str): JSON text representation.

        Returns:
            JsonObject: JSON object.
        """
        if str_repr == "null":
            return cls()

        return cls(json.loads(str_repr))

    def serialize(self):
        """Return the object text representation.

        Returns:
            str: JSON object text representation.
        """
        if self._is_null:
            return None

        if self._is_array:
            return json.dumps(self._array_value, sort_keys=True, separators=(",", ":"))

        return json.dumps(self, sort_keys=True, separators=(",", ":"))


class ProtoDeserializer:
    """
    Provides functionality of deserializing valid string into
    a Proto Message and valid int into a Proto Enum value.
    """

    @classmethod
    def to_proto_message(cls, bytes_string, proto_message_object):
        """parses serialized protocol buffer data into proto message.

        Args:
            bytes_string (str): string of bytes.
            proto_message_object (Message): Message object for parsing

        Returns:
            Message: parses serialized protocol buffer data into this message.

        Raises:
            ValueError: if the input proto_message_object is not of type Message
        """
        if not isinstance(proto_message_object, Message):
            raise ValueError("Input proto_message_object should be of type Message")

        proto_message = proto_message_object.__deepcopy__()
        proto_message.ParseFromString(bytes_string)
        return proto_message

    @classmethod
    def to_proto_enum(cls, int_value, proto_enum_object):
        """parses int value into string containing the name of an enum value.

        Args:
            int_value (int): integer value.
            proto_enum_object (EnumTypeWrapper): Enum object.

        Returns:
            str: string containing the name of an enum value.

        Raises:
            ValueError: if the input proto_enum_object is not of type EnumTypeWrapper
        """
        if not isinstance(proto_enum_object, EnumTypeWrapper):
            raise ValueError("Input proto_enum_object should be of type EnumTypeWrapper")

        return proto_enum_object.Name(int_value)

    @classmethod
    def to_proto_message_list(cls, bytes_string_list, proto_message_object):
        """parses list of serialized protocol buffer data into proto message list.

        Args:
            bytes_string_list (list[str]): list of string of bytes.
            proto_message_object (Message): Message object for parsing

        Returns:
            list[Message]: parses list of serialized protocol buffer data into list of message.

        Raises:
            ValueError: if the input bytes_string_list is not of type list
        """
        if not isinstance(bytes_string_list, (list, tuple)):
            raise ValueError("Expected input bytes_string_list to be a list of strings")

        proto_message_list = [cls.to_proto_message(item, proto_message_object) for item in bytes_string_list]
        return proto_message_list

    @classmethod
    def to_proto_enum_list(cls, int_list, proto_enum_object):
        """parses int value list into list of enum values.

        Args:
            int_list (list[int]): list of integer value.
            proto_enum_object (EnumTypeWrapper): Enum object.

        Returns:
            list[str]: list of strings containing the name of enum value.

        Raises:
            ValueError: if the input int_list is not of type list
        """
        if not isinstance(int_list, (list, tuple)):
            raise ValueError("Expected input int_list to be a list of int")

        proto_enum_list = [cls.to_proto_enum(item, proto_enum_object) for item in int_list]
        return proto_enum_list
