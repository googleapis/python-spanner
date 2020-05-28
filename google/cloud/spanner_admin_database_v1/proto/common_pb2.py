# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/cloud/spanner_admin_database_v1/proto/common.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.api import field_behavior_pb2 as google_dot_api_dot_field__behavior__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.api import annotations_pb2 as google_dot_api_dot_annotations__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
    name="google/cloud/spanner_admin_database_v1/proto/common.proto",
    package="google.spanner.admin.database.v1",
    syntax="proto3",
    serialized_options=b"\n$com.google.spanner.admin.database.v1B\013CommonProtoP\001ZHgoogle.golang.org/genproto/googleapis/spanner/admin/database/v1;database\252\002&Google.Cloud.Spanner.Admin.Database.V1\312\002&Google\\Cloud\\Spanner\\Admin\\Database\\V1",
    create_key=_descriptor._internal_create_key,
    serialized_pb=b'\n9google/cloud/spanner_admin_database_v1/proto/common.proto\x12 google.spanner.admin.database.v1\x1a\x1fgoogle/api/field_behavior.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1cgoogle/api/annotations.proto"\x8b\x01\n\x11OperationProgress\x12\x18\n\x10progress_percent\x18\x01 \x01(\x05\x12.\n\nstart_time\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08\x65nd_time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.TimestampB\xd1\x01\n$com.google.spanner.admin.database.v1B\x0b\x43ommonProtoP\x01ZHgoogle.golang.org/genproto/googleapis/spanner/admin/database/v1;database\xaa\x02&Google.Cloud.Spanner.Admin.Database.V1\xca\x02&Google\\Cloud\\Spanner\\Admin\\Database\\V1b\x06proto3',
    dependencies=[
        google_dot_api_dot_field__behavior__pb2.DESCRIPTOR,
        google_dot_protobuf_dot_timestamp__pb2.DESCRIPTOR,
        google_dot_api_dot_annotations__pb2.DESCRIPTOR,
    ],
)


_OPERATIONPROGRESS = _descriptor.Descriptor(
    name="OperationProgress",
    full_name="google.spanner.admin.database.v1.OperationProgress",
    filename=None,
    file=DESCRIPTOR,
    containing_type=None,
    create_key=_descriptor._internal_create_key,
    fields=[
        _descriptor.FieldDescriptor(
            name="progress_percent",
            full_name="google.spanner.admin.database.v1.OperationProgress.progress_percent",
            index=0,
            number=1,
            type=5,
            cpp_type=1,
            label=1,
            has_default_value=False,
            default_value=0,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="start_time",
            full_name="google.spanner.admin.database.v1.OperationProgress.start_time",
            index=1,
            number=2,
            type=11,
            cpp_type=10,
            label=1,
            has_default_value=False,
            default_value=None,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
        _descriptor.FieldDescriptor(
            name="end_time",
            full_name="google.spanner.admin.database.v1.OperationProgress.end_time",
            index=2,
            number=3,
            type=11,
            cpp_type=10,
            label=1,
            has_default_value=False,
            default_value=None,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
            create_key=_descriptor._internal_create_key,
        ),
    ],
    extensions=[],
    nested_types=[],
    enum_types=[],
    serialized_options=None,
    is_extendable=False,
    syntax="proto3",
    extension_ranges=[],
    oneofs=[],
    serialized_start=192,
    serialized_end=331,
)

_OPERATIONPROGRESS.fields_by_name[
    "start_time"
].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
_OPERATIONPROGRESS.fields_by_name[
    "end_time"
].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
DESCRIPTOR.message_types_by_name["OperationProgress"] = _OPERATIONPROGRESS
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

OperationProgress = _reflection.GeneratedProtocolMessageType(
    "OperationProgress",
    (_message.Message,),
    {
        "DESCRIPTOR": _OPERATIONPROGRESS,
        "__module__": "google.cloud.spanner_admin_database_v1.proto.common_pb2",
        "__doc__": """Encapsulates progress related information for a Cloud Spanner long
  running operation.
  Attributes:
      progress_percent:
          Percent completion of the operation. Values are between 0 and
          100 inclusive.
      start_time:
          Time the request was received.
      end_time:
          If set, the time at which this operation failed or was
          completed successfully.
  """,
        # @@protoc_insertion_point(class_scope:google.spanner.admin.database.v1.OperationProgress)
    },
)
_sym_db.RegisterMessage(OperationProgress)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)
