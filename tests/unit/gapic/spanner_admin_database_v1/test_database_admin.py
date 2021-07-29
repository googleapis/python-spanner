# -*- coding: utf-8 -*-
# Copyright 2020 Google LLC
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
#
import os
import mock
import packaging.version

import grpc
from grpc.experimental import aio
import math
import pytest
from proto.marshal.rules.dates import DurationRule, TimestampRule


from google.api_core import client_options
from google.api_core import exceptions as core_exceptions
from google.api_core import future
from google.api_core import gapic_v1
from google.api_core import grpc_helpers
from google.api_core import grpc_helpers_async
from google.api_core import operation_async  # type: ignore
from google.api_core import operations_v1
from google.auth import credentials as ga_credentials
from google.auth.exceptions import MutualTLSChannelError
from google.cloud.spanner_admin_database_v1.services.database_admin import (
    DatabaseAdminAsyncClient,
)
from google.cloud.spanner_admin_database_v1.services.database_admin import (
    DatabaseAdminClient,
)
from google.cloud.spanner_admin_database_v1.services.database_admin import pagers
from google.cloud.spanner_admin_database_v1.services.database_admin import transports
from google.cloud.spanner_admin_database_v1.services.database_admin.transports.base import (
    _GOOGLE_AUTH_VERSION,
)
from google.cloud.spanner_admin_database_v1.types import backup
from google.cloud.spanner_admin_database_v1.types import backup as gsad_backup
from google.cloud.spanner_admin_database_v1.types import common
from google.cloud.spanner_admin_database_v1.types import spanner_database_admin
from google.iam.v1 import iam_policy_pb2  # type: ignore
from google.iam.v1 import options_pb2  # type: ignore
from google.iam.v1 import policy_pb2  # type: ignore
from google.longrunning import operations_pb2
from google.longrunning import operations_pb2  # type: ignore
from google.oauth2 import service_account
from google.protobuf import any_pb2  # type: ignore
from google.protobuf import field_mask_pb2  # type: ignore
from google.protobuf import timestamp_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore
from google.type import expr_pb2  # type: ignore
import google.auth


# TODO(busunkim): Once google-auth >= 1.25.0 is required transitively
# through google-api-core:
# - Delete the auth "less than" test cases
# - Delete these pytest markers (Make the "greater than or equal to" tests the default).
requires_google_auth_lt_1_25_0 = pytest.mark.skipif(
    packaging.version.parse(_GOOGLE_AUTH_VERSION) >= packaging.version.parse("1.25.0"),
    reason="This test requires google-auth < 1.25.0",
)
requires_google_auth_gte_1_25_0 = pytest.mark.skipif(
    packaging.version.parse(_GOOGLE_AUTH_VERSION) < packaging.version.parse("1.25.0"),
    reason="This test requires google-auth >= 1.25.0",
)


def client_cert_source_callback():
    return b"cert bytes", b"key bytes"


# If default endpoint is localhost, then default mtls endpoint will be the same.
# This method modifies the default endpoint so the client can produce a different
# mtls endpoint for endpoint testing purposes.
def modify_default_endpoint(client):
    return (
        "foo.googleapis.com"
        if ("localhost" in client.DEFAULT_ENDPOINT)
        else client.DEFAULT_ENDPOINT
    )


def test__get_default_mtls_endpoint():
    api_endpoint = "example.googleapis.com"
    api_mtls_endpoint = "example.mtls.googleapis.com"
    sandbox_endpoint = "example.sandbox.googleapis.com"
    sandbox_mtls_endpoint = "example.mtls.sandbox.googleapis.com"
    non_googleapi = "api.example.com"

    assert DatabaseAdminClient._get_default_mtls_endpoint(None) is None
    assert (
        DatabaseAdminClient._get_default_mtls_endpoint(api_endpoint)
        == api_mtls_endpoint
    )
    assert (
        DatabaseAdminClient._get_default_mtls_endpoint(api_mtls_endpoint)
        == api_mtls_endpoint
    )
    assert (
        DatabaseAdminClient._get_default_mtls_endpoint(sandbox_endpoint)
        == sandbox_mtls_endpoint
    )
    assert (
        DatabaseAdminClient._get_default_mtls_endpoint(sandbox_mtls_endpoint)
        == sandbox_mtls_endpoint
    )
    assert (
        DatabaseAdminClient._get_default_mtls_endpoint(non_googleapi) == non_googleapi
    )


@pytest.mark.parametrize(
    "client_class", [DatabaseAdminClient, DatabaseAdminAsyncClient,]
)
def test_database_admin_client_from_service_account_info(client_class):
    creds = ga_credentials.AnonymousCredentials()
    with mock.patch.object(
        service_account.Credentials, "from_service_account_info"
    ) as factory:
        factory.return_value = creds
        info = {"valid": True}
        client = client_class.from_service_account_info(info)
        assert client.transport._credentials == creds
        assert isinstance(client, client_class)

        assert client.transport._host == "spanner.googleapis.com:443"


@pytest.mark.parametrize(
    "transport_class,transport_name",
    [
        (transports.DatabaseAdminGrpcTransport, "grpc"),
        (transports.DatabaseAdminGrpcAsyncIOTransport, "grpc_asyncio"),
    ],
)
def test_database_admin_client_service_account_always_use_jwt(
    transport_class, transport_name
):
    with mock.patch.object(
        service_account.Credentials, "with_always_use_jwt_access", create=True
    ) as use_jwt:
        creds = service_account.Credentials(None, None, None)
        transport = transport_class(credentials=creds, always_use_jwt_access=True)
        use_jwt.assert_called_once_with(True)

    with mock.patch.object(
        service_account.Credentials, "with_always_use_jwt_access", create=True
    ) as use_jwt:
        creds = service_account.Credentials(None, None, None)
        transport = transport_class(credentials=creds, always_use_jwt_access=False)
        use_jwt.assert_not_called()


@pytest.mark.parametrize(
    "client_class", [DatabaseAdminClient, DatabaseAdminAsyncClient,]
)
def test_database_admin_client_from_service_account_file(client_class):
    creds = ga_credentials.AnonymousCredentials()
    with mock.patch.object(
        service_account.Credentials, "from_service_account_file"
    ) as factory:
        factory.return_value = creds
        client = client_class.from_service_account_file("dummy/file/path.json")
        assert client.transport._credentials == creds
        assert isinstance(client, client_class)

        client = client_class.from_service_account_json("dummy/file/path.json")
        assert client.transport._credentials == creds
        assert isinstance(client, client_class)

        assert client.transport._host == "spanner.googleapis.com:443"


def test_database_admin_client_get_transport_class():
    transport = DatabaseAdminClient.get_transport_class()
    available_transports = [
        transports.DatabaseAdminGrpcTransport,
    ]
    assert transport in available_transports

    transport = DatabaseAdminClient.get_transport_class("grpc")
    assert transport == transports.DatabaseAdminGrpcTransport


@pytest.mark.parametrize(
    "client_class,transport_class,transport_name",
    [
        (DatabaseAdminClient, transports.DatabaseAdminGrpcTransport, "grpc"),
        (
            DatabaseAdminAsyncClient,
            transports.DatabaseAdminGrpcAsyncIOTransport,
            "grpc_asyncio",
        ),
    ],
)
@mock.patch.object(
    DatabaseAdminClient,
    "DEFAULT_ENDPOINT",
    modify_default_endpoint(DatabaseAdminClient),
)
@mock.patch.object(
    DatabaseAdminAsyncClient,
    "DEFAULT_ENDPOINT",
    modify_default_endpoint(DatabaseAdminAsyncClient),
)
def test_database_admin_client_client_options(
    client_class, transport_class, transport_name
):
    # Check that if channel is provided we won't create a new one.
    with mock.patch.object(DatabaseAdminClient, "get_transport_class") as gtc:
        transport = transport_class(credentials=ga_credentials.AnonymousCredentials())
        client = client_class(transport=transport)
        gtc.assert_not_called()

    # Check that if channel is provided via str we will create a new one.
    with mock.patch.object(DatabaseAdminClient, "get_transport_class") as gtc:
        client = client_class(transport=transport_name)
        gtc.assert_called()

    # Check the case api_endpoint is provided.
    options = client_options.ClientOptions(api_endpoint="squid.clam.whelk")
    with mock.patch.object(transport_class, "__init__") as patched:
        patched.return_value = None
        client = client_class(client_options=options)
        patched.assert_called_once_with(
            credentials=None,
            credentials_file=None,
            host="squid.clam.whelk",
            scopes=None,
            client_cert_source_for_mtls=None,
            quota_project_id=None,
            client_info=transports.base.DEFAULT_CLIENT_INFO,
            always_use_jwt_access=True,
        )

    # Check the case api_endpoint is not provided and GOOGLE_API_USE_MTLS_ENDPOINT is
    # "never".
    with mock.patch.dict(os.environ, {"GOOGLE_API_USE_MTLS_ENDPOINT": "never"}):
        with mock.patch.object(transport_class, "__init__") as patched:
            patched.return_value = None
            client = client_class()
            patched.assert_called_once_with(
                credentials=None,
                credentials_file=None,
                host=client.DEFAULT_ENDPOINT,
                scopes=None,
                client_cert_source_for_mtls=None,
                quota_project_id=None,
                client_info=transports.base.DEFAULT_CLIENT_INFO,
                always_use_jwt_access=True,
            )

    # Check the case api_endpoint is not provided and GOOGLE_API_USE_MTLS_ENDPOINT is
    # "always".
    with mock.patch.dict(os.environ, {"GOOGLE_API_USE_MTLS_ENDPOINT": "always"}):
        with mock.patch.object(transport_class, "__init__") as patched:
            patched.return_value = None
            client = client_class()
            patched.assert_called_once_with(
                credentials=None,
                credentials_file=None,
                host=client.DEFAULT_MTLS_ENDPOINT,
                scopes=None,
                client_cert_source_for_mtls=None,
                quota_project_id=None,
                client_info=transports.base.DEFAULT_CLIENT_INFO,
                always_use_jwt_access=True,
            )

    # Check the case api_endpoint is not provided and GOOGLE_API_USE_MTLS_ENDPOINT has
    # unsupported value.
    with mock.patch.dict(os.environ, {"GOOGLE_API_USE_MTLS_ENDPOINT": "Unsupported"}):
        with pytest.raises(MutualTLSChannelError):
            client = client_class()

    # Check the case GOOGLE_API_USE_CLIENT_CERTIFICATE has unsupported value.
    with mock.patch.dict(
        os.environ, {"GOOGLE_API_USE_CLIENT_CERTIFICATE": "Unsupported"}
    ):
        with pytest.raises(ValueError):
            client = client_class()

    # Check the case quota_project_id is provided
    options = client_options.ClientOptions(quota_project_id="octopus")
    with mock.patch.object(transport_class, "__init__") as patched:
        patched.return_value = None
        client = client_class(client_options=options)
        patched.assert_called_once_with(
            credentials=None,
            credentials_file=None,
            host=client.DEFAULT_ENDPOINT,
            scopes=None,
            client_cert_source_for_mtls=None,
            quota_project_id="octopus",
            client_info=transports.base.DEFAULT_CLIENT_INFO,
            always_use_jwt_access=True,
        )


@pytest.mark.parametrize(
    "client_class,transport_class,transport_name,use_client_cert_env",
    [
        (DatabaseAdminClient, transports.DatabaseAdminGrpcTransport, "grpc", "true"),
        (
            DatabaseAdminAsyncClient,
            transports.DatabaseAdminGrpcAsyncIOTransport,
            "grpc_asyncio",
            "true",
        ),
        (DatabaseAdminClient, transports.DatabaseAdminGrpcTransport, "grpc", "false"),
        (
            DatabaseAdminAsyncClient,
            transports.DatabaseAdminGrpcAsyncIOTransport,
            "grpc_asyncio",
            "false",
        ),
    ],
)
@mock.patch.object(
    DatabaseAdminClient,
    "DEFAULT_ENDPOINT",
    modify_default_endpoint(DatabaseAdminClient),
)
@mock.patch.object(
    DatabaseAdminAsyncClient,
    "DEFAULT_ENDPOINT",
    modify_default_endpoint(DatabaseAdminAsyncClient),
)
@mock.patch.dict(os.environ, {"GOOGLE_API_USE_MTLS_ENDPOINT": "auto"})
def test_database_admin_client_mtls_env_auto(
    client_class, transport_class, transport_name, use_client_cert_env
):
    # This tests the endpoint autoswitch behavior. Endpoint is autoswitched to the default
    # mtls endpoint, if GOOGLE_API_USE_CLIENT_CERTIFICATE is "true" and client cert exists.

    # Check the case client_cert_source is provided. Whether client cert is used depends on
    # GOOGLE_API_USE_CLIENT_CERTIFICATE value.
    with mock.patch.dict(
        os.environ, {"GOOGLE_API_USE_CLIENT_CERTIFICATE": use_client_cert_env}
    ):
        options = client_options.ClientOptions(
            client_cert_source=client_cert_source_callback
        )
        with mock.patch.object(transport_class, "__init__") as patched:
            patched.return_value = None
            client = client_class(client_options=options)

            if use_client_cert_env == "false":
                expected_client_cert_source = None
                expected_host = client.DEFAULT_ENDPOINT
            else:
                expected_client_cert_source = client_cert_source_callback
                expected_host = client.DEFAULT_MTLS_ENDPOINT

            patched.assert_called_once_with(
                credentials=None,
                credentials_file=None,
                host=expected_host,
                scopes=None,
                client_cert_source_for_mtls=expected_client_cert_source,
                quota_project_id=None,
                client_info=transports.base.DEFAULT_CLIENT_INFO,
                always_use_jwt_access=True,
            )

    # Check the case ADC client cert is provided. Whether client cert is used depends on
    # GOOGLE_API_USE_CLIENT_CERTIFICATE value.
    with mock.patch.dict(
        os.environ, {"GOOGLE_API_USE_CLIENT_CERTIFICATE": use_client_cert_env}
    ):
        with mock.patch.object(transport_class, "__init__") as patched:
            with mock.patch(
                "google.auth.transport.mtls.has_default_client_cert_source",
                return_value=True,
            ):
                with mock.patch(
                    "google.auth.transport.mtls.default_client_cert_source",
                    return_value=client_cert_source_callback,
                ):
                    if use_client_cert_env == "false":
                        expected_host = client.DEFAULT_ENDPOINT
                        expected_client_cert_source = None
                    else:
                        expected_host = client.DEFAULT_MTLS_ENDPOINT
                        expected_client_cert_source = client_cert_source_callback

                    patched.return_value = None
                    client = client_class()
                    patched.assert_called_once_with(
                        credentials=None,
                        credentials_file=None,
                        host=expected_host,
                        scopes=None,
                        client_cert_source_for_mtls=expected_client_cert_source,
                        quota_project_id=None,
                        client_info=transports.base.DEFAULT_CLIENT_INFO,
                        always_use_jwt_access=True,
                    )

    # Check the case client_cert_source and ADC client cert are not provided.
    with mock.patch.dict(
        os.environ, {"GOOGLE_API_USE_CLIENT_CERTIFICATE": use_client_cert_env}
    ):
        with mock.patch.object(transport_class, "__init__") as patched:
            with mock.patch(
                "google.auth.transport.mtls.has_default_client_cert_source",
                return_value=False,
            ):
                patched.return_value = None
                client = client_class()
                patched.assert_called_once_with(
                    credentials=None,
                    credentials_file=None,
                    host=client.DEFAULT_ENDPOINT,
                    scopes=None,
                    client_cert_source_for_mtls=None,
                    quota_project_id=None,
                    client_info=transports.base.DEFAULT_CLIENT_INFO,
                    always_use_jwt_access=True,
                )


@pytest.mark.parametrize(
    "client_class,transport_class,transport_name",
    [
        (DatabaseAdminClient, transports.DatabaseAdminGrpcTransport, "grpc"),
        (
            DatabaseAdminAsyncClient,
            transports.DatabaseAdminGrpcAsyncIOTransport,
            "grpc_asyncio",
        ),
    ],
)
def test_database_admin_client_client_options_scopes(
    client_class, transport_class, transport_name
):
    # Check the case scopes are provided.
    options = client_options.ClientOptions(scopes=["1", "2"],)
    with mock.patch.object(transport_class, "__init__") as patched:
        patched.return_value = None
        client = client_class(client_options=options)
        patched.assert_called_once_with(
            credentials=None,
            credentials_file=None,
            host=client.DEFAULT_ENDPOINT,
            scopes=["1", "2"],
            client_cert_source_for_mtls=None,
            quota_project_id=None,
            client_info=transports.base.DEFAULT_CLIENT_INFO,
            always_use_jwt_access=True,
        )


@pytest.mark.parametrize(
    "client_class,transport_class,transport_name",
    [
        (DatabaseAdminClient, transports.DatabaseAdminGrpcTransport, "grpc"),
        (
            DatabaseAdminAsyncClient,
            transports.DatabaseAdminGrpcAsyncIOTransport,
            "grpc_asyncio",
        ),
    ],
)
def test_database_admin_client_client_options_credentials_file(
    client_class, transport_class, transport_name
):
    # Check the case credentials file is provided.
    options = client_options.ClientOptions(credentials_file="credentials.json")
    with mock.patch.object(transport_class, "__init__") as patched:
        patched.return_value = None
        client = client_class(client_options=options)
        patched.assert_called_once_with(
            credentials=None,
            credentials_file="credentials.json",
            host=client.DEFAULT_ENDPOINT,
            scopes=None,
            client_cert_source_for_mtls=None,
            quota_project_id=None,
            client_info=transports.base.DEFAULT_CLIENT_INFO,
            always_use_jwt_access=True,
        )


def test_database_admin_client_client_options_from_dict():
    with mock.patch(
        "google.cloud.spanner_admin_database_v1.services.database_admin.transports.DatabaseAdminGrpcTransport.__init__"
    ) as grpc_transport:
        grpc_transport.return_value = None
        client = DatabaseAdminClient(
            client_options={"api_endpoint": "squid.clam.whelk"}
        )
        grpc_transport.assert_called_once_with(
            credentials=None,
            credentials_file=None,
            host="squid.clam.whelk",
            scopes=None,
            client_cert_source_for_mtls=None,
            quota_project_id=None,
            client_info=transports.base.DEFAULT_CLIENT_INFO,
            always_use_jwt_access=True,
        )


def test_list_databases(
    transport: str = "grpc", request_type=spanner_database_admin.ListDatabasesRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.ListDatabasesResponse(
            next_page_token="next_page_token_value",
        )
        response = client.list_databases(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.ListDatabasesRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, pagers.ListDatabasesPager)
    assert response.next_page_token == "next_page_token_value"


def test_list_databases_from_dict():
    test_list_databases(request_type=dict)


def test_list_databases_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        client.list_databases()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.ListDatabasesRequest()


@pytest.mark.asyncio
async def test_list_databases_async(
    transport: str = "grpc_asyncio",
    request_type=spanner_database_admin.ListDatabasesRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.ListDatabasesResponse(
                next_page_token="next_page_token_value",
            )
        )
        response = await client.list_databases(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.ListDatabasesRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, pagers.ListDatabasesAsyncPager)
    assert response.next_page_token == "next_page_token_value"


@pytest.mark.asyncio
async def test_list_databases_async_from_dict():
    await test_list_databases_async(request_type=dict)


def test_list_databases_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.ListDatabasesRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        call.return_value = spanner_database_admin.ListDatabasesResponse()
        client.list_databases(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_list_databases_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.ListDatabasesRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.ListDatabasesResponse()
        )
        await client.list_databases(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


def test_list_databases_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.ListDatabasesResponse()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.list_databases(parent="parent_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"


def test_list_databases_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.list_databases(
            spanner_database_admin.ListDatabasesRequest(), parent="parent_value",
        )


@pytest.mark.asyncio
async def test_list_databases_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.ListDatabasesResponse()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.ListDatabasesResponse()
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.list_databases(parent="parent_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"


@pytest.mark.asyncio
async def test_list_databases_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.list_databases(
            spanner_database_admin.ListDatabasesRequest(), parent="parent_value",
        )


def test_list_databases_pager():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        # Set the response to a series of pages.
        call.side_effect = (
            spanner_database_admin.ListDatabasesResponse(
                databases=[
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                ],
                next_page_token="abc",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[], next_page_token="def",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[spanner_database_admin.Database(),], next_page_token="ghi",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                ],
            ),
            RuntimeError,
        )

        metadata = ()
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata((("parent", ""),)),
        )
        pager = client.list_databases(request={})

        assert pager._metadata == metadata

        results = [i for i in pager]
        assert len(results) == 6
        assert all(isinstance(i, spanner_database_admin.Database) for i in results)


def test_list_databases_pages():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_databases), "__call__") as call:
        # Set the response to a series of pages.
        call.side_effect = (
            spanner_database_admin.ListDatabasesResponse(
                databases=[
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                ],
                next_page_token="abc",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[], next_page_token="def",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[spanner_database_admin.Database(),], next_page_token="ghi",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                ],
            ),
            RuntimeError,
        )
        pages = list(client.list_databases(request={}).pages)
        for page_, token in zip(pages, ["abc", "def", "ghi", ""]):
            assert page_.raw_page.next_page_token == token


@pytest.mark.asyncio
async def test_list_databases_async_pager():
    client = DatabaseAdminAsyncClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_databases), "__call__", new_callable=mock.AsyncMock
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            spanner_database_admin.ListDatabasesResponse(
                databases=[
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                ],
                next_page_token="abc",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[], next_page_token="def",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[spanner_database_admin.Database(),], next_page_token="ghi",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                ],
            ),
            RuntimeError,
        )
        async_pager = await client.list_databases(request={},)
        assert async_pager.next_page_token == "abc"
        responses = []
        async for response in async_pager:
            responses.append(response)

        assert len(responses) == 6
        assert all(isinstance(i, spanner_database_admin.Database) for i in responses)


@pytest.mark.asyncio
async def test_list_databases_async_pages():
    client = DatabaseAdminAsyncClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_databases), "__call__", new_callable=mock.AsyncMock
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            spanner_database_admin.ListDatabasesResponse(
                databases=[
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                ],
                next_page_token="abc",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[], next_page_token="def",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[spanner_database_admin.Database(),], next_page_token="ghi",
            ),
            spanner_database_admin.ListDatabasesResponse(
                databases=[
                    spanner_database_admin.Database(),
                    spanner_database_admin.Database(),
                ],
            ),
            RuntimeError,
        )
        pages = []
        async for page_ in (await client.list_databases(request={})).pages:
            pages.append(page_)
        for page_, token in zip(pages, ["abc", "def", "ghi", ""]):
            assert page_.raw_page.next_page_token == token


def test_create_database(
    transport: str = "grpc", request_type=spanner_database_admin.CreateDatabaseRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/spam")
        response = client.create_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.CreateDatabaseRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, future.Future)


def test_create_database_from_dict():
    test_create_database(request_type=dict)


def test_create_database_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_database), "__call__") as call:
        client.create_database()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.CreateDatabaseRequest()


@pytest.mark.asyncio
async def test_create_database_async(
    transport: str = "grpc_asyncio",
    request_type=spanner_database_admin.CreateDatabaseRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/spam")
        )
        response = await client.create_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.CreateDatabaseRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, future.Future)


@pytest.mark.asyncio
async def test_create_database_async_from_dict():
    await test_create_database_async(request_type=dict)


def test_create_database_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.CreateDatabaseRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_database), "__call__") as call:
        call.return_value = operations_pb2.Operation(name="operations/op")
        client.create_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_create_database_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.CreateDatabaseRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_database), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/op")
        )
        await client.create_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


def test_create_database_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/op")
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.create_database(
            parent="parent_value", create_statement="create_statement_value",
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"
        assert args[0].create_statement == "create_statement_value"


def test_create_database_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.create_database(
            spanner_database_admin.CreateDatabaseRequest(),
            parent="parent_value",
            create_statement="create_statement_value",
        )


@pytest.mark.asyncio
async def test_create_database_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/op")

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/spam")
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.create_database(
            parent="parent_value", create_statement="create_statement_value",
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"
        assert args[0].create_statement == "create_statement_value"


@pytest.mark.asyncio
async def test_create_database_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.create_database(
            spanner_database_admin.CreateDatabaseRequest(),
            parent="parent_value",
            create_statement="create_statement_value",
        )


def test_get_database(
    transport: str = "grpc", request_type=spanner_database_admin.GetDatabaseRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.Database(
            name="name_value",
            state=spanner_database_admin.Database.State.CREATING,
            version_retention_period="version_retention_period_value",
            default_leader="default_leader_value",
        )
        response = client.get_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.GetDatabaseRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, spanner_database_admin.Database)
    assert response.name == "name_value"
    assert response.state == spanner_database_admin.Database.State.CREATING
    assert response.version_retention_period == "version_retention_period_value"
    assert response.default_leader == "default_leader_value"


def test_get_database_from_dict():
    test_get_database(request_type=dict)


def test_get_database_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database), "__call__") as call:
        client.get_database()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.GetDatabaseRequest()


@pytest.mark.asyncio
async def test_get_database_async(
    transport: str = "grpc_asyncio",
    request_type=spanner_database_admin.GetDatabaseRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.Database(
                name="name_value",
                state=spanner_database_admin.Database.State.CREATING,
                version_retention_period="version_retention_period_value",
                default_leader="default_leader_value",
            )
        )
        response = await client.get_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.GetDatabaseRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, spanner_database_admin.Database)
    assert response.name == "name_value"
    assert response.state == spanner_database_admin.Database.State.CREATING
    assert response.version_retention_period == "version_retention_period_value"
    assert response.default_leader == "default_leader_value"


@pytest.mark.asyncio
async def test_get_database_async_from_dict():
    await test_get_database_async(request_type=dict)


def test_get_database_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.GetDatabaseRequest()

    request.name = "name/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database), "__call__") as call:
        call.return_value = spanner_database_admin.Database()
        client.get_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "name=name/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_get_database_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.GetDatabaseRequest()

    request.name = "name/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.Database()
        )
        await client.get_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "name=name/value",) in kw["metadata"]


def test_get_database_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.Database()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.get_database(name="name_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].name == "name_value"


def test_get_database_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.get_database(
            spanner_database_admin.GetDatabaseRequest(), name="name_value",
        )


@pytest.mark.asyncio
async def test_get_database_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.Database()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.Database()
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.get_database(name="name_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].name == "name_value"


@pytest.mark.asyncio
async def test_get_database_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.get_database(
            spanner_database_admin.GetDatabaseRequest(), name="name_value",
        )


def test_update_database_ddl(
    transport: str = "grpc",
    request_type=spanner_database_admin.UpdateDatabaseDdlRequest,
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.update_database_ddl), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/spam")
        response = client.update_database_ddl(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.UpdateDatabaseDdlRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, future.Future)


def test_update_database_ddl_from_dict():
    test_update_database_ddl(request_type=dict)


def test_update_database_ddl_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.update_database_ddl), "__call__"
    ) as call:
        client.update_database_ddl()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.UpdateDatabaseDdlRequest()


@pytest.mark.asyncio
async def test_update_database_ddl_async(
    transport: str = "grpc_asyncio",
    request_type=spanner_database_admin.UpdateDatabaseDdlRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.update_database_ddl), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/spam")
        )
        response = await client.update_database_ddl(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.UpdateDatabaseDdlRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, future.Future)


@pytest.mark.asyncio
async def test_update_database_ddl_async_from_dict():
    await test_update_database_ddl_async(request_type=dict)


def test_update_database_ddl_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.UpdateDatabaseDdlRequest()

    request.database = "database/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.update_database_ddl), "__call__"
    ) as call:
        call.return_value = operations_pb2.Operation(name="operations/op")
        client.update_database_ddl(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "database=database/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_update_database_ddl_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.UpdateDatabaseDdlRequest()

    request.database = "database/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.update_database_ddl), "__call__"
    ) as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/op")
        )
        await client.update_database_ddl(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "database=database/value",) in kw["metadata"]


def test_update_database_ddl_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.update_database_ddl), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/op")
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.update_database_ddl(
            database="database_value", statements=["statements_value"],
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].database == "database_value"
        assert args[0].statements == ["statements_value"]


def test_update_database_ddl_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.update_database_ddl(
            spanner_database_admin.UpdateDatabaseDdlRequest(),
            database="database_value",
            statements=["statements_value"],
        )


@pytest.mark.asyncio
async def test_update_database_ddl_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.update_database_ddl), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/op")

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/spam")
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.update_database_ddl(
            database="database_value", statements=["statements_value"],
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].database == "database_value"
        assert args[0].statements == ["statements_value"]


@pytest.mark.asyncio
async def test_update_database_ddl_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.update_database_ddl(
            spanner_database_admin.UpdateDatabaseDdlRequest(),
            database="database_value",
            statements=["statements_value"],
        )


def test_drop_database(
    transport: str = "grpc", request_type=spanner_database_admin.DropDatabaseRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.drop_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = None
        response = client.drop_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.DropDatabaseRequest()

    # Establish that the response is the type that we expect.
    assert response is None


def test_drop_database_from_dict():
    test_drop_database(request_type=dict)


def test_drop_database_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.drop_database), "__call__") as call:
        client.drop_database()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.DropDatabaseRequest()


@pytest.mark.asyncio
async def test_drop_database_async(
    transport: str = "grpc_asyncio",
    request_type=spanner_database_admin.DropDatabaseRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.drop_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(None)
        response = await client.drop_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.DropDatabaseRequest()

    # Establish that the response is the type that we expect.
    assert response is None


@pytest.mark.asyncio
async def test_drop_database_async_from_dict():
    await test_drop_database_async(request_type=dict)


def test_drop_database_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.DropDatabaseRequest()

    request.database = "database/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.drop_database), "__call__") as call:
        call.return_value = None
        client.drop_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "database=database/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_drop_database_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.DropDatabaseRequest()

    request.database = "database/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.drop_database), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(None)
        await client.drop_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "database=database/value",) in kw["metadata"]


def test_drop_database_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.drop_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = None
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.drop_database(database="database_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].database == "database_value"


def test_drop_database_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.drop_database(
            spanner_database_admin.DropDatabaseRequest(), database="database_value",
        )


@pytest.mark.asyncio
async def test_drop_database_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.drop_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = None

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(None)
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.drop_database(database="database_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].database == "database_value"


@pytest.mark.asyncio
async def test_drop_database_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.drop_database(
            spanner_database_admin.DropDatabaseRequest(), database="database_value",
        )


def test_get_database_ddl(
    transport: str = "grpc", request_type=spanner_database_admin.GetDatabaseDdlRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database_ddl), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.GetDatabaseDdlResponse(
            statements=["statements_value"],
        )
        response = client.get_database_ddl(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.GetDatabaseDdlRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, spanner_database_admin.GetDatabaseDdlResponse)
    assert response.statements == ["statements_value"]


def test_get_database_ddl_from_dict():
    test_get_database_ddl(request_type=dict)


def test_get_database_ddl_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database_ddl), "__call__") as call:
        client.get_database_ddl()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.GetDatabaseDdlRequest()


@pytest.mark.asyncio
async def test_get_database_ddl_async(
    transport: str = "grpc_asyncio",
    request_type=spanner_database_admin.GetDatabaseDdlRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database_ddl), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.GetDatabaseDdlResponse(
                statements=["statements_value"],
            )
        )
        response = await client.get_database_ddl(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.GetDatabaseDdlRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, spanner_database_admin.GetDatabaseDdlResponse)
    assert response.statements == ["statements_value"]


@pytest.mark.asyncio
async def test_get_database_ddl_async_from_dict():
    await test_get_database_ddl_async(request_type=dict)


def test_get_database_ddl_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.GetDatabaseDdlRequest()

    request.database = "database/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database_ddl), "__call__") as call:
        call.return_value = spanner_database_admin.GetDatabaseDdlResponse()
        client.get_database_ddl(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "database=database/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_get_database_ddl_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.GetDatabaseDdlRequest()

    request.database = "database/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database_ddl), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.GetDatabaseDdlResponse()
        )
        await client.get_database_ddl(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "database=database/value",) in kw["metadata"]


def test_get_database_ddl_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database_ddl), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.GetDatabaseDdlResponse()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.get_database_ddl(database="database_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].database == "database_value"


def test_get_database_ddl_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.get_database_ddl(
            spanner_database_admin.GetDatabaseDdlRequest(), database="database_value",
        )


@pytest.mark.asyncio
async def test_get_database_ddl_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_database_ddl), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.GetDatabaseDdlResponse()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.GetDatabaseDdlResponse()
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.get_database_ddl(database="database_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].database == "database_value"


@pytest.mark.asyncio
async def test_get_database_ddl_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.get_database_ddl(
            spanner_database_admin.GetDatabaseDdlRequest(), database="database_value",
        )


def test_set_iam_policy(
    transport: str = "grpc", request_type=iam_policy_pb2.SetIamPolicyRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.set_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = policy_pb2.Policy(version=774, etag=b"etag_blob",)
        response = client.set_iam_policy(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.SetIamPolicyRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, policy_pb2.Policy)
    assert response.version == 774
    assert response.etag == b"etag_blob"


def test_set_iam_policy_from_dict():
    test_set_iam_policy(request_type=dict)


def test_set_iam_policy_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.set_iam_policy), "__call__") as call:
        client.set_iam_policy()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.SetIamPolicyRequest()


@pytest.mark.asyncio
async def test_set_iam_policy_async(
    transport: str = "grpc_asyncio", request_type=iam_policy_pb2.SetIamPolicyRequest
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.set_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            policy_pb2.Policy(version=774, etag=b"etag_blob",)
        )
        response = await client.set_iam_policy(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.SetIamPolicyRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, policy_pb2.Policy)
    assert response.version == 774
    assert response.etag == b"etag_blob"


@pytest.mark.asyncio
async def test_set_iam_policy_async_from_dict():
    await test_set_iam_policy_async(request_type=dict)


def test_set_iam_policy_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = iam_policy_pb2.SetIamPolicyRequest()

    request.resource = "resource/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.set_iam_policy), "__call__") as call:
        call.return_value = policy_pb2.Policy()
        client.set_iam_policy(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "resource=resource/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_set_iam_policy_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = iam_policy_pb2.SetIamPolicyRequest()

    request.resource = "resource/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.set_iam_policy), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(policy_pb2.Policy())
        await client.set_iam_policy(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "resource=resource/value",) in kw["metadata"]


def test_set_iam_policy_from_dict_foreign():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)
    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.set_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = policy_pb2.Policy()
        response = client.set_iam_policy(
            request={
                "resource": "resource_value",
                "policy": policy_pb2.Policy(version=774),
            }
        )
        call.assert_called()


def test_set_iam_policy_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.set_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = policy_pb2.Policy()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.set_iam_policy(resource="resource_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].resource == "resource_value"


def test_set_iam_policy_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.set_iam_policy(
            iam_policy_pb2.SetIamPolicyRequest(), resource="resource_value",
        )


@pytest.mark.asyncio
async def test_set_iam_policy_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.set_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = policy_pb2.Policy()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(policy_pb2.Policy())
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.set_iam_policy(resource="resource_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].resource == "resource_value"


@pytest.mark.asyncio
async def test_set_iam_policy_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.set_iam_policy(
            iam_policy_pb2.SetIamPolicyRequest(), resource="resource_value",
        )


def test_get_iam_policy(
    transport: str = "grpc", request_type=iam_policy_pb2.GetIamPolicyRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = policy_pb2.Policy(version=774, etag=b"etag_blob",)
        response = client.get_iam_policy(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.GetIamPolicyRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, policy_pb2.Policy)
    assert response.version == 774
    assert response.etag == b"etag_blob"


def test_get_iam_policy_from_dict():
    test_get_iam_policy(request_type=dict)


def test_get_iam_policy_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_iam_policy), "__call__") as call:
        client.get_iam_policy()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.GetIamPolicyRequest()


@pytest.mark.asyncio
async def test_get_iam_policy_async(
    transport: str = "grpc_asyncio", request_type=iam_policy_pb2.GetIamPolicyRequest
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            policy_pb2.Policy(version=774, etag=b"etag_blob",)
        )
        response = await client.get_iam_policy(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.GetIamPolicyRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, policy_pb2.Policy)
    assert response.version == 774
    assert response.etag == b"etag_blob"


@pytest.mark.asyncio
async def test_get_iam_policy_async_from_dict():
    await test_get_iam_policy_async(request_type=dict)


def test_get_iam_policy_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = iam_policy_pb2.GetIamPolicyRequest()

    request.resource = "resource/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_iam_policy), "__call__") as call:
        call.return_value = policy_pb2.Policy()
        client.get_iam_policy(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "resource=resource/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_get_iam_policy_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = iam_policy_pb2.GetIamPolicyRequest()

    request.resource = "resource/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_iam_policy), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(policy_pb2.Policy())
        await client.get_iam_policy(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "resource=resource/value",) in kw["metadata"]


def test_get_iam_policy_from_dict_foreign():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)
    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = policy_pb2.Policy()
        response = client.get_iam_policy(
            request={
                "resource": "resource_value",
                "options": options_pb2.GetPolicyOptions(requested_policy_version=2598),
            }
        )
        call.assert_called()


def test_get_iam_policy_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = policy_pb2.Policy()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.get_iam_policy(resource="resource_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].resource == "resource_value"


def test_get_iam_policy_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.get_iam_policy(
            iam_policy_pb2.GetIamPolicyRequest(), resource="resource_value",
        )


@pytest.mark.asyncio
async def test_get_iam_policy_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_iam_policy), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = policy_pb2.Policy()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(policy_pb2.Policy())
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.get_iam_policy(resource="resource_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].resource == "resource_value"


@pytest.mark.asyncio
async def test_get_iam_policy_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.get_iam_policy(
            iam_policy_pb2.GetIamPolicyRequest(), resource="resource_value",
        )


def test_test_iam_permissions(
    transport: str = "grpc", request_type=iam_policy_pb2.TestIamPermissionsRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.test_iam_permissions), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = iam_policy_pb2.TestIamPermissionsResponse(
            permissions=["permissions_value"],
        )
        response = client.test_iam_permissions(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.TestIamPermissionsRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, iam_policy_pb2.TestIamPermissionsResponse)
    assert response.permissions == ["permissions_value"]


def test_test_iam_permissions_from_dict():
    test_test_iam_permissions(request_type=dict)


def test_test_iam_permissions_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.test_iam_permissions), "__call__"
    ) as call:
        client.test_iam_permissions()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.TestIamPermissionsRequest()


@pytest.mark.asyncio
async def test_test_iam_permissions_async(
    transport: str = "grpc_asyncio",
    request_type=iam_policy_pb2.TestIamPermissionsRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.test_iam_permissions), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            iam_policy_pb2.TestIamPermissionsResponse(
                permissions=["permissions_value"],
            )
        )
        response = await client.test_iam_permissions(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == iam_policy_pb2.TestIamPermissionsRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, iam_policy_pb2.TestIamPermissionsResponse)
    assert response.permissions == ["permissions_value"]


@pytest.mark.asyncio
async def test_test_iam_permissions_async_from_dict():
    await test_test_iam_permissions_async(request_type=dict)


def test_test_iam_permissions_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = iam_policy_pb2.TestIamPermissionsRequest()

    request.resource = "resource/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.test_iam_permissions), "__call__"
    ) as call:
        call.return_value = iam_policy_pb2.TestIamPermissionsResponse()
        client.test_iam_permissions(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "resource=resource/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_test_iam_permissions_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = iam_policy_pb2.TestIamPermissionsRequest()

    request.resource = "resource/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.test_iam_permissions), "__call__"
    ) as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            iam_policy_pb2.TestIamPermissionsResponse()
        )
        await client.test_iam_permissions(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "resource=resource/value",) in kw["metadata"]


def test_test_iam_permissions_from_dict_foreign():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)
    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.test_iam_permissions), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = iam_policy_pb2.TestIamPermissionsResponse()
        response = client.test_iam_permissions(
            request={
                "resource": "resource_value",
                "permissions": ["permissions_value"],
            }
        )
        call.assert_called()


def test_test_iam_permissions_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.test_iam_permissions), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = iam_policy_pb2.TestIamPermissionsResponse()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.test_iam_permissions(
            resource="resource_value", permissions=["permissions_value"],
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].resource == "resource_value"
        assert args[0].permissions == ["permissions_value"]


def test_test_iam_permissions_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.test_iam_permissions(
            iam_policy_pb2.TestIamPermissionsRequest(),
            resource="resource_value",
            permissions=["permissions_value"],
        )


@pytest.mark.asyncio
async def test_test_iam_permissions_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.test_iam_permissions), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = iam_policy_pb2.TestIamPermissionsResponse()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            iam_policy_pb2.TestIamPermissionsResponse()
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.test_iam_permissions(
            resource="resource_value", permissions=["permissions_value"],
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].resource == "resource_value"
        assert args[0].permissions == ["permissions_value"]


@pytest.mark.asyncio
async def test_test_iam_permissions_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.test_iam_permissions(
            iam_policy_pb2.TestIamPermissionsRequest(),
            resource="resource_value",
            permissions=["permissions_value"],
        )


def test_create_backup(
    transport: str = "grpc", request_type=gsad_backup.CreateBackupRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/spam")
        response = client.create_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == gsad_backup.CreateBackupRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, future.Future)


def test_create_backup_from_dict():
    test_create_backup(request_type=dict)


def test_create_backup_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_backup), "__call__") as call:
        client.create_backup()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == gsad_backup.CreateBackupRequest()


@pytest.mark.asyncio
async def test_create_backup_async(
    transport: str = "grpc_asyncio", request_type=gsad_backup.CreateBackupRequest
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/spam")
        )
        response = await client.create_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == gsad_backup.CreateBackupRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, future.Future)


@pytest.mark.asyncio
async def test_create_backup_async_from_dict():
    await test_create_backup_async(request_type=dict)


def test_create_backup_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = gsad_backup.CreateBackupRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_backup), "__call__") as call:
        call.return_value = operations_pb2.Operation(name="operations/op")
        client.create_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_create_backup_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = gsad_backup.CreateBackupRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_backup), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/op")
        )
        await client.create_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


def test_create_backup_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/op")
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.create_backup(
            parent="parent_value",
            backup=gsad_backup.Backup(database="database_value"),
            backup_id="backup_id_value",
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"
        assert args[0].backup == gsad_backup.Backup(database="database_value")
        assert args[0].backup_id == "backup_id_value"


def test_create_backup_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.create_backup(
            gsad_backup.CreateBackupRequest(),
            parent="parent_value",
            backup=gsad_backup.Backup(database="database_value"),
            backup_id="backup_id_value",
        )


@pytest.mark.asyncio
async def test_create_backup_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.create_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/op")

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/spam")
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.create_backup(
            parent="parent_value",
            backup=gsad_backup.Backup(database="database_value"),
            backup_id="backup_id_value",
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"
        assert args[0].backup == gsad_backup.Backup(database="database_value")
        assert args[0].backup_id == "backup_id_value"


@pytest.mark.asyncio
async def test_create_backup_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.create_backup(
            gsad_backup.CreateBackupRequest(),
            parent="parent_value",
            backup=gsad_backup.Backup(database="database_value"),
            backup_id="backup_id_value",
        )


def test_get_backup(transport: str = "grpc", request_type=backup.GetBackupRequest):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.Backup(
            database="database_value",
            name="name_value",
            size_bytes=1089,
            state=backup.Backup.State.CREATING,
            referencing_databases=["referencing_databases_value"],
        )
        response = client.get_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.GetBackupRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, backup.Backup)
    assert response.database == "database_value"
    assert response.name == "name_value"
    assert response.size_bytes == 1089
    assert response.state == backup.Backup.State.CREATING
    assert response.referencing_databases == ["referencing_databases_value"]


def test_get_backup_from_dict():
    test_get_backup(request_type=dict)


def test_get_backup_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_backup), "__call__") as call:
        client.get_backup()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.GetBackupRequest()


@pytest.mark.asyncio
async def test_get_backup_async(
    transport: str = "grpc_asyncio", request_type=backup.GetBackupRequest
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            backup.Backup(
                database="database_value",
                name="name_value",
                size_bytes=1089,
                state=backup.Backup.State.CREATING,
                referencing_databases=["referencing_databases_value"],
            )
        )
        response = await client.get_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.GetBackupRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, backup.Backup)
    assert response.database == "database_value"
    assert response.name == "name_value"
    assert response.size_bytes == 1089
    assert response.state == backup.Backup.State.CREATING
    assert response.referencing_databases == ["referencing_databases_value"]


@pytest.mark.asyncio
async def test_get_backup_async_from_dict():
    await test_get_backup_async(request_type=dict)


def test_get_backup_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = backup.GetBackupRequest()

    request.name = "name/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_backup), "__call__") as call:
        call.return_value = backup.Backup()
        client.get_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "name=name/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_get_backup_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = backup.GetBackupRequest()

    request.name = "name/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_backup), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(backup.Backup())
        await client.get_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "name=name/value",) in kw["metadata"]


def test_get_backup_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.Backup()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.get_backup(name="name_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].name == "name_value"


def test_get_backup_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.get_backup(
            backup.GetBackupRequest(), name="name_value",
        )


@pytest.mark.asyncio
async def test_get_backup_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.get_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.Backup()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(backup.Backup())
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.get_backup(name="name_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].name == "name_value"


@pytest.mark.asyncio
async def test_get_backup_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.get_backup(
            backup.GetBackupRequest(), name="name_value",
        )


def test_update_backup(
    transport: str = "grpc", request_type=gsad_backup.UpdateBackupRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.update_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = gsad_backup.Backup(
            database="database_value",
            name="name_value",
            size_bytes=1089,
            state=gsad_backup.Backup.State.CREATING,
            referencing_databases=["referencing_databases_value"],
        )
        response = client.update_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == gsad_backup.UpdateBackupRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, gsad_backup.Backup)
    assert response.database == "database_value"
    assert response.name == "name_value"
    assert response.size_bytes == 1089
    assert response.state == gsad_backup.Backup.State.CREATING
    assert response.referencing_databases == ["referencing_databases_value"]


def test_update_backup_from_dict():
    test_update_backup(request_type=dict)


def test_update_backup_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.update_backup), "__call__") as call:
        client.update_backup()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == gsad_backup.UpdateBackupRequest()


@pytest.mark.asyncio
async def test_update_backup_async(
    transport: str = "grpc_asyncio", request_type=gsad_backup.UpdateBackupRequest
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.update_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            gsad_backup.Backup(
                database="database_value",
                name="name_value",
                size_bytes=1089,
                state=gsad_backup.Backup.State.CREATING,
                referencing_databases=["referencing_databases_value"],
            )
        )
        response = await client.update_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == gsad_backup.UpdateBackupRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, gsad_backup.Backup)
    assert response.database == "database_value"
    assert response.name == "name_value"
    assert response.size_bytes == 1089
    assert response.state == gsad_backup.Backup.State.CREATING
    assert response.referencing_databases == ["referencing_databases_value"]


@pytest.mark.asyncio
async def test_update_backup_async_from_dict():
    await test_update_backup_async(request_type=dict)


def test_update_backup_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = gsad_backup.UpdateBackupRequest()

    request.backup.name = "backup.name/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.update_backup), "__call__") as call:
        call.return_value = gsad_backup.Backup()
        client.update_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "backup.name=backup.name/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_update_backup_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = gsad_backup.UpdateBackupRequest()

    request.backup.name = "backup.name/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.update_backup), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(gsad_backup.Backup())
        await client.update_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "backup.name=backup.name/value",) in kw["metadata"]


def test_update_backup_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.update_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = gsad_backup.Backup()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.update_backup(
            backup=gsad_backup.Backup(database="database_value"),
            update_mask=field_mask_pb2.FieldMask(paths=["paths_value"]),
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].backup == gsad_backup.Backup(database="database_value")
        assert args[0].update_mask == field_mask_pb2.FieldMask(paths=["paths_value"])


def test_update_backup_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.update_backup(
            gsad_backup.UpdateBackupRequest(),
            backup=gsad_backup.Backup(database="database_value"),
            update_mask=field_mask_pb2.FieldMask(paths=["paths_value"]),
        )


@pytest.mark.asyncio
async def test_update_backup_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.update_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = gsad_backup.Backup()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(gsad_backup.Backup())
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.update_backup(
            backup=gsad_backup.Backup(database="database_value"),
            update_mask=field_mask_pb2.FieldMask(paths=["paths_value"]),
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].backup == gsad_backup.Backup(database="database_value")
        assert args[0].update_mask == field_mask_pb2.FieldMask(paths=["paths_value"])


@pytest.mark.asyncio
async def test_update_backup_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.update_backup(
            gsad_backup.UpdateBackupRequest(),
            backup=gsad_backup.Backup(database="database_value"),
            update_mask=field_mask_pb2.FieldMask(paths=["paths_value"]),
        )


def test_delete_backup(
    transport: str = "grpc", request_type=backup.DeleteBackupRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.delete_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = None
        response = client.delete_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.DeleteBackupRequest()

    # Establish that the response is the type that we expect.
    assert response is None


def test_delete_backup_from_dict():
    test_delete_backup(request_type=dict)


def test_delete_backup_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.delete_backup), "__call__") as call:
        client.delete_backup()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.DeleteBackupRequest()


@pytest.mark.asyncio
async def test_delete_backup_async(
    transport: str = "grpc_asyncio", request_type=backup.DeleteBackupRequest
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.delete_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(None)
        response = await client.delete_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.DeleteBackupRequest()

    # Establish that the response is the type that we expect.
    assert response is None


@pytest.mark.asyncio
async def test_delete_backup_async_from_dict():
    await test_delete_backup_async(request_type=dict)


def test_delete_backup_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = backup.DeleteBackupRequest()

    request.name = "name/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.delete_backup), "__call__") as call:
        call.return_value = None
        client.delete_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "name=name/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_delete_backup_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = backup.DeleteBackupRequest()

    request.name = "name/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.delete_backup), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(None)
        await client.delete_backup(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "name=name/value",) in kw["metadata"]


def test_delete_backup_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.delete_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = None
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.delete_backup(name="name_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].name == "name_value"


def test_delete_backup_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.delete_backup(
            backup.DeleteBackupRequest(), name="name_value",
        )


@pytest.mark.asyncio
async def test_delete_backup_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.delete_backup), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = None

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(None)
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.delete_backup(name="name_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].name == "name_value"


@pytest.mark.asyncio
async def test_delete_backup_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.delete_backup(
            backup.DeleteBackupRequest(), name="name_value",
        )


def test_list_backups(transport: str = "grpc", request_type=backup.ListBackupsRequest):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.ListBackupsResponse(
            next_page_token="next_page_token_value",
        )
        response = client.list_backups(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.ListBackupsRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, pagers.ListBackupsPager)
    assert response.next_page_token == "next_page_token_value"


def test_list_backups_from_dict():
    test_list_backups(request_type=dict)


def test_list_backups_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        client.list_backups()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.ListBackupsRequest()


@pytest.mark.asyncio
async def test_list_backups_async(
    transport: str = "grpc_asyncio", request_type=backup.ListBackupsRequest
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            backup.ListBackupsResponse(next_page_token="next_page_token_value",)
        )
        response = await client.list_backups(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.ListBackupsRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, pagers.ListBackupsAsyncPager)
    assert response.next_page_token == "next_page_token_value"


@pytest.mark.asyncio
async def test_list_backups_async_from_dict():
    await test_list_backups_async(request_type=dict)


def test_list_backups_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = backup.ListBackupsRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        call.return_value = backup.ListBackupsResponse()
        client.list_backups(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_list_backups_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = backup.ListBackupsRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            backup.ListBackupsResponse()
        )
        await client.list_backups(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


def test_list_backups_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.ListBackupsResponse()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.list_backups(parent="parent_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"


def test_list_backups_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.list_backups(
            backup.ListBackupsRequest(), parent="parent_value",
        )


@pytest.mark.asyncio
async def test_list_backups_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.ListBackupsResponse()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            backup.ListBackupsResponse()
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.list_backups(parent="parent_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"


@pytest.mark.asyncio
async def test_list_backups_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.list_backups(
            backup.ListBackupsRequest(), parent="parent_value",
        )


def test_list_backups_pager():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        # Set the response to a series of pages.
        call.side_effect = (
            backup.ListBackupsResponse(
                backups=[backup.Backup(), backup.Backup(), backup.Backup(),],
                next_page_token="abc",
            ),
            backup.ListBackupsResponse(backups=[], next_page_token="def",),
            backup.ListBackupsResponse(
                backups=[backup.Backup(),], next_page_token="ghi",
            ),
            backup.ListBackupsResponse(backups=[backup.Backup(), backup.Backup(),],),
            RuntimeError,
        )

        metadata = ()
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata((("parent", ""),)),
        )
        pager = client.list_backups(request={})

        assert pager._metadata == metadata

        results = [i for i in pager]
        assert len(results) == 6
        assert all(isinstance(i, backup.Backup) for i in results)


def test_list_backups_pages():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.list_backups), "__call__") as call:
        # Set the response to a series of pages.
        call.side_effect = (
            backup.ListBackupsResponse(
                backups=[backup.Backup(), backup.Backup(), backup.Backup(),],
                next_page_token="abc",
            ),
            backup.ListBackupsResponse(backups=[], next_page_token="def",),
            backup.ListBackupsResponse(
                backups=[backup.Backup(),], next_page_token="ghi",
            ),
            backup.ListBackupsResponse(backups=[backup.Backup(), backup.Backup(),],),
            RuntimeError,
        )
        pages = list(client.list_backups(request={}).pages)
        for page_, token in zip(pages, ["abc", "def", "ghi", ""]):
            assert page_.raw_page.next_page_token == token


@pytest.mark.asyncio
async def test_list_backups_async_pager():
    client = DatabaseAdminAsyncClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backups), "__call__", new_callable=mock.AsyncMock
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            backup.ListBackupsResponse(
                backups=[backup.Backup(), backup.Backup(), backup.Backup(),],
                next_page_token="abc",
            ),
            backup.ListBackupsResponse(backups=[], next_page_token="def",),
            backup.ListBackupsResponse(
                backups=[backup.Backup(),], next_page_token="ghi",
            ),
            backup.ListBackupsResponse(backups=[backup.Backup(), backup.Backup(),],),
            RuntimeError,
        )
        async_pager = await client.list_backups(request={},)
        assert async_pager.next_page_token == "abc"
        responses = []
        async for response in async_pager:
            responses.append(response)

        assert len(responses) == 6
        assert all(isinstance(i, backup.Backup) for i in responses)


@pytest.mark.asyncio
async def test_list_backups_async_pages():
    client = DatabaseAdminAsyncClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backups), "__call__", new_callable=mock.AsyncMock
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            backup.ListBackupsResponse(
                backups=[backup.Backup(), backup.Backup(), backup.Backup(),],
                next_page_token="abc",
            ),
            backup.ListBackupsResponse(backups=[], next_page_token="def",),
            backup.ListBackupsResponse(
                backups=[backup.Backup(),], next_page_token="ghi",
            ),
            backup.ListBackupsResponse(backups=[backup.Backup(), backup.Backup(),],),
            RuntimeError,
        )
        pages = []
        async for page_ in (await client.list_backups(request={})).pages:
            pages.append(page_)
        for page_, token in zip(pages, ["abc", "def", "ghi", ""]):
            assert page_.raw_page.next_page_token == token


def test_restore_database(
    transport: str = "grpc", request_type=spanner_database_admin.RestoreDatabaseRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.restore_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/spam")
        response = client.restore_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.RestoreDatabaseRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, future.Future)


def test_restore_database_from_dict():
    test_restore_database(request_type=dict)


def test_restore_database_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.restore_database), "__call__") as call:
        client.restore_database()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.RestoreDatabaseRequest()


@pytest.mark.asyncio
async def test_restore_database_async(
    transport: str = "grpc_asyncio",
    request_type=spanner_database_admin.RestoreDatabaseRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.restore_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/spam")
        )
        response = await client.restore_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.RestoreDatabaseRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, future.Future)


@pytest.mark.asyncio
async def test_restore_database_async_from_dict():
    await test_restore_database_async(request_type=dict)


def test_restore_database_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.RestoreDatabaseRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.restore_database), "__call__") as call:
        call.return_value = operations_pb2.Operation(name="operations/op")
        client.restore_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_restore_database_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.RestoreDatabaseRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.restore_database), "__call__") as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/op")
        )
        await client.restore_database(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


def test_restore_database_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.restore_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/op")
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.restore_database(
            parent="parent_value",
            database_id="database_id_value",
            backup="backup_value",
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"
        assert args[0].database_id == "database_id_value"
        assert args[0].backup == "backup_value"


def test_restore_database_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.restore_database(
            spanner_database_admin.RestoreDatabaseRequest(),
            parent="parent_value",
            database_id="database_id_value",
            backup="backup_value",
        )


@pytest.mark.asyncio
async def test_restore_database_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(type(client.transport.restore_database), "__call__") as call:
        # Designate an appropriate return value for the call.
        call.return_value = operations_pb2.Operation(name="operations/op")

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            operations_pb2.Operation(name="operations/spam")
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.restore_database(
            parent="parent_value",
            database_id="database_id_value",
            backup="backup_value",
        )

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"
        assert args[0].database_id == "database_id_value"
        assert args[0].backup == "backup_value"


@pytest.mark.asyncio
async def test_restore_database_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.restore_database(
            spanner_database_admin.RestoreDatabaseRequest(),
            parent="parent_value",
            database_id="database_id_value",
            backup="backup_value",
        )


def test_list_database_operations(
    transport: str = "grpc",
    request_type=spanner_database_admin.ListDatabaseOperationsRequest,
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.ListDatabaseOperationsResponse(
            next_page_token="next_page_token_value",
        )
        response = client.list_database_operations(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.ListDatabaseOperationsRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, pagers.ListDatabaseOperationsPager)
    assert response.next_page_token == "next_page_token_value"


def test_list_database_operations_from_dict():
    test_list_database_operations(request_type=dict)


def test_list_database_operations_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        client.list_database_operations()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.ListDatabaseOperationsRequest()


@pytest.mark.asyncio
async def test_list_database_operations_async(
    transport: str = "grpc_asyncio",
    request_type=spanner_database_admin.ListDatabaseOperationsRequest,
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.ListDatabaseOperationsResponse(
                next_page_token="next_page_token_value",
            )
        )
        response = await client.list_database_operations(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == spanner_database_admin.ListDatabaseOperationsRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, pagers.ListDatabaseOperationsAsyncPager)
    assert response.next_page_token == "next_page_token_value"


@pytest.mark.asyncio
async def test_list_database_operations_async_from_dict():
    await test_list_database_operations_async(request_type=dict)


def test_list_database_operations_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.ListDatabaseOperationsRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        call.return_value = spanner_database_admin.ListDatabaseOperationsResponse()
        client.list_database_operations(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_list_database_operations_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = spanner_database_admin.ListDatabaseOperationsRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.ListDatabaseOperationsResponse()
        )
        await client.list_database_operations(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


def test_list_database_operations_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.ListDatabaseOperationsResponse()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.list_database_operations(parent="parent_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"


def test_list_database_operations_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.list_database_operations(
            spanner_database_admin.ListDatabaseOperationsRequest(),
            parent="parent_value",
        )


@pytest.mark.asyncio
async def test_list_database_operations_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = spanner_database_admin.ListDatabaseOperationsResponse()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            spanner_database_admin.ListDatabaseOperationsResponse()
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.list_database_operations(parent="parent_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"


@pytest.mark.asyncio
async def test_list_database_operations_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.list_database_operations(
            spanner_database_admin.ListDatabaseOperationsRequest(),
            parent="parent_value",
        )


def test_list_database_operations_pager():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                ],
                next_page_token="abc",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[], next_page_token="def",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[operations_pb2.Operation(),], next_page_token="ghi",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[operations_pb2.Operation(), operations_pb2.Operation(),],
            ),
            RuntimeError,
        )

        metadata = ()
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata((("parent", ""),)),
        )
        pager = client.list_database_operations(request={})

        assert pager._metadata == metadata

        results = [i for i in pager]
        assert len(results) == 6
        assert all(isinstance(i, operations_pb2.Operation) for i in results)


def test_list_database_operations_pages():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations), "__call__"
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                ],
                next_page_token="abc",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[], next_page_token="def",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[operations_pb2.Operation(),], next_page_token="ghi",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[operations_pb2.Operation(), operations_pb2.Operation(),],
            ),
            RuntimeError,
        )
        pages = list(client.list_database_operations(request={}).pages)
        for page_, token in zip(pages, ["abc", "def", "ghi", ""]):
            assert page_.raw_page.next_page_token == token


@pytest.mark.asyncio
async def test_list_database_operations_async_pager():
    client = DatabaseAdminAsyncClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations),
        "__call__",
        new_callable=mock.AsyncMock,
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                ],
                next_page_token="abc",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[], next_page_token="def",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[operations_pb2.Operation(),], next_page_token="ghi",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[operations_pb2.Operation(), operations_pb2.Operation(),],
            ),
            RuntimeError,
        )
        async_pager = await client.list_database_operations(request={},)
        assert async_pager.next_page_token == "abc"
        responses = []
        async for response in async_pager:
            responses.append(response)

        assert len(responses) == 6
        assert all(isinstance(i, operations_pb2.Operation) for i in responses)


@pytest.mark.asyncio
async def test_list_database_operations_async_pages():
    client = DatabaseAdminAsyncClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_database_operations),
        "__call__",
        new_callable=mock.AsyncMock,
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                ],
                next_page_token="abc",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[], next_page_token="def",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[operations_pb2.Operation(),], next_page_token="ghi",
            ),
            spanner_database_admin.ListDatabaseOperationsResponse(
                operations=[operations_pb2.Operation(), operations_pb2.Operation(),],
            ),
            RuntimeError,
        )
        pages = []
        async for page_ in (await client.list_database_operations(request={})).pages:
            pages.append(page_)
        for page_, token in zip(pages, ["abc", "def", "ghi", ""]):
            assert page_.raw_page.next_page_token == token


def test_list_backup_operations(
    transport: str = "grpc", request_type=backup.ListBackupOperationsRequest
):
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.ListBackupOperationsResponse(
            next_page_token="next_page_token_value",
        )
        response = client.list_backup_operations(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.ListBackupOperationsRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, pagers.ListBackupOperationsPager)
    assert response.next_page_token == "next_page_token_value"


def test_list_backup_operations_from_dict():
    test_list_backup_operations(request_type=dict)


def test_list_backup_operations_empty_call():
    # This test is a coverage failsafe to make sure that totally empty calls,
    # i.e. request == None and no flattened fields passed, work.
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        client.list_backup_operations()
        call.assert_called()
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.ListBackupOperationsRequest()


@pytest.mark.asyncio
async def test_list_backup_operations_async(
    transport: str = "grpc_asyncio", request_type=backup.ListBackupOperationsRequest
):
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport=transport,
    )

    # Everything is optional in proto3 as far as the runtime is concerned,
    # and we are mocking out the actual API, so just send an empty request.
    request = request_type()

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            backup.ListBackupOperationsResponse(
                next_page_token="next_page_token_value",
            )
        )
        response = await client.list_backup_operations(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == backup.ListBackupOperationsRequest()

    # Establish that the response is the type that we expect.
    assert isinstance(response, pagers.ListBackupOperationsAsyncPager)
    assert response.next_page_token == "next_page_token_value"


@pytest.mark.asyncio
async def test_list_backup_operations_async_from_dict():
    await test_list_backup_operations_async(request_type=dict)


def test_list_backup_operations_field_headers():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = backup.ListBackupOperationsRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        call.return_value = backup.ListBackupOperationsResponse()
        client.list_backup_operations(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


@pytest.mark.asyncio
async def test_list_backup_operations_field_headers_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Any value that is part of the HTTP/1.1 URI should be sent as
    # a field header. Set these to a non-empty value.
    request = backup.ListBackupOperationsRequest()

    request.parent = "parent/value"

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            backup.ListBackupOperationsResponse()
        )
        await client.list_backup_operations(request)

        # Establish that the underlying gRPC stub method was called.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0] == request

    # Establish that the field header was sent.
    _, _, kw = call.mock_calls[0]
    assert ("x-goog-request-params", "parent=parent/value",) in kw["metadata"]


def test_list_backup_operations_flattened():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.ListBackupOperationsResponse()
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        client.list_backup_operations(parent="parent_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls) == 1
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"


def test_list_backup_operations_flattened_error():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        client.list_backup_operations(
            backup.ListBackupOperationsRequest(), parent="parent_value",
        )


@pytest.mark.asyncio
async def test_list_backup_operations_flattened_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        # Designate an appropriate return value for the call.
        call.return_value = backup.ListBackupOperationsResponse()

        call.return_value = grpc_helpers_async.FakeUnaryUnaryCall(
            backup.ListBackupOperationsResponse()
        )
        # Call the method with a truthy value for each flattened field,
        # using the keyword arguments to the method.
        response = await client.list_backup_operations(parent="parent_value",)

        # Establish that the underlying call was made with the expected
        # request object values.
        assert len(call.mock_calls)
        _, args, _ = call.mock_calls[0]
        assert args[0].parent == "parent_value"


@pytest.mark.asyncio
async def test_list_backup_operations_flattened_error_async():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(),
    )

    # Attempting to call a method with both a request object and flattened
    # fields is an error.
    with pytest.raises(ValueError):
        await client.list_backup_operations(
            backup.ListBackupOperationsRequest(), parent="parent_value",
        )


def test_list_backup_operations_pager():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            backup.ListBackupOperationsResponse(
                operations=[
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                ],
                next_page_token="abc",
            ),
            backup.ListBackupOperationsResponse(operations=[], next_page_token="def",),
            backup.ListBackupOperationsResponse(
                operations=[operations_pb2.Operation(),], next_page_token="ghi",
            ),
            backup.ListBackupOperationsResponse(
                operations=[operations_pb2.Operation(), operations_pb2.Operation(),],
            ),
            RuntimeError,
        )

        metadata = ()
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata((("parent", ""),)),
        )
        pager = client.list_backup_operations(request={})

        assert pager._metadata == metadata

        results = [i for i in pager]
        assert len(results) == 6
        assert all(isinstance(i, operations_pb2.Operation) for i in results)


def test_list_backup_operations_pages():
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations), "__call__"
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            backup.ListBackupOperationsResponse(
                operations=[
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                ],
                next_page_token="abc",
            ),
            backup.ListBackupOperationsResponse(operations=[], next_page_token="def",),
            backup.ListBackupOperationsResponse(
                operations=[operations_pb2.Operation(),], next_page_token="ghi",
            ),
            backup.ListBackupOperationsResponse(
                operations=[operations_pb2.Operation(), operations_pb2.Operation(),],
            ),
            RuntimeError,
        )
        pages = list(client.list_backup_operations(request={}).pages)
        for page_, token in zip(pages, ["abc", "def", "ghi", ""]):
            assert page_.raw_page.next_page_token == token


@pytest.mark.asyncio
async def test_list_backup_operations_async_pager():
    client = DatabaseAdminAsyncClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations),
        "__call__",
        new_callable=mock.AsyncMock,
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            backup.ListBackupOperationsResponse(
                operations=[
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                ],
                next_page_token="abc",
            ),
            backup.ListBackupOperationsResponse(operations=[], next_page_token="def",),
            backup.ListBackupOperationsResponse(
                operations=[operations_pb2.Operation(),], next_page_token="ghi",
            ),
            backup.ListBackupOperationsResponse(
                operations=[operations_pb2.Operation(), operations_pb2.Operation(),],
            ),
            RuntimeError,
        )
        async_pager = await client.list_backup_operations(request={},)
        assert async_pager.next_page_token == "abc"
        responses = []
        async for response in async_pager:
            responses.append(response)

        assert len(responses) == 6
        assert all(isinstance(i, operations_pb2.Operation) for i in responses)


@pytest.mark.asyncio
async def test_list_backup_operations_async_pages():
    client = DatabaseAdminAsyncClient(credentials=ga_credentials.AnonymousCredentials,)

    # Mock the actual call within the gRPC stub, and fake the request.
    with mock.patch.object(
        type(client.transport.list_backup_operations),
        "__call__",
        new_callable=mock.AsyncMock,
    ) as call:
        # Set the response to a series of pages.
        call.side_effect = (
            backup.ListBackupOperationsResponse(
                operations=[
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                    operations_pb2.Operation(),
                ],
                next_page_token="abc",
            ),
            backup.ListBackupOperationsResponse(operations=[], next_page_token="def",),
            backup.ListBackupOperationsResponse(
                operations=[operations_pb2.Operation(),], next_page_token="ghi",
            ),
            backup.ListBackupOperationsResponse(
                operations=[operations_pb2.Operation(), operations_pb2.Operation(),],
            ),
            RuntimeError,
        )
        pages = []
        async for page_ in (await client.list_backup_operations(request={})).pages:
            pages.append(page_)
        for page_, token in zip(pages, ["abc", "def", "ghi", ""]):
            assert page_.raw_page.next_page_token == token


def test_credentials_transport_error():
    # It is an error to provide credentials and a transport instance.
    transport = transports.DatabaseAdminGrpcTransport(
        credentials=ga_credentials.AnonymousCredentials(),
    )
    with pytest.raises(ValueError):
        client = DatabaseAdminClient(
            credentials=ga_credentials.AnonymousCredentials(), transport=transport,
        )

    # It is an error to provide a credentials file and a transport instance.
    transport = transports.DatabaseAdminGrpcTransport(
        credentials=ga_credentials.AnonymousCredentials(),
    )
    with pytest.raises(ValueError):
        client = DatabaseAdminClient(
            client_options={"credentials_file": "credentials.json"},
            transport=transport,
        )

    # It is an error to provide scopes and a transport instance.
    transport = transports.DatabaseAdminGrpcTransport(
        credentials=ga_credentials.AnonymousCredentials(),
    )
    with pytest.raises(ValueError):
        client = DatabaseAdminClient(
            client_options={"scopes": ["1", "2"]}, transport=transport,
        )


def test_transport_instance():
    # A client may be instantiated with a custom transport instance.
    transport = transports.DatabaseAdminGrpcTransport(
        credentials=ga_credentials.AnonymousCredentials(),
    )
    client = DatabaseAdminClient(transport=transport)
    assert client.transport is transport


def test_transport_get_channel():
    # A client may be instantiated with a custom transport instance.
    transport = transports.DatabaseAdminGrpcTransport(
        credentials=ga_credentials.AnonymousCredentials(),
    )
    channel = transport.grpc_channel
    assert channel

    transport = transports.DatabaseAdminGrpcAsyncIOTransport(
        credentials=ga_credentials.AnonymousCredentials(),
    )
    channel = transport.grpc_channel
    assert channel


@pytest.mark.parametrize(
    "transport_class",
    [
        transports.DatabaseAdminGrpcTransport,
        transports.DatabaseAdminGrpcAsyncIOTransport,
    ],
)
def test_transport_adc(transport_class):
    # Test default credentials are used if not provided.
    with mock.patch.object(google.auth, "default") as adc:
        adc.return_value = (ga_credentials.AnonymousCredentials(), None)
        transport_class()
        adc.assert_called_once()


def test_transport_grpc_default():
    # A client should use the gRPC transport by default.
    client = DatabaseAdminClient(credentials=ga_credentials.AnonymousCredentials(),)
    assert isinstance(client.transport, transports.DatabaseAdminGrpcTransport,)


def test_database_admin_base_transport_error():
    # Passing both a credentials object and credentials_file should raise an error
    with pytest.raises(core_exceptions.DuplicateCredentialArgs):
        transport = transports.DatabaseAdminTransport(
            credentials=ga_credentials.AnonymousCredentials(),
            credentials_file="credentials.json",
        )


def test_database_admin_base_transport():
    # Instantiate the base transport.
    with mock.patch(
        "google.cloud.spanner_admin_database_v1.services.database_admin.transports.DatabaseAdminTransport.__init__"
    ) as Transport:
        Transport.return_value = None
        transport = transports.DatabaseAdminTransport(
            credentials=ga_credentials.AnonymousCredentials(),
        )

    # Every method on the transport should just blindly
    # raise NotImplementedError.
    methods = (
        "list_databases",
        "create_database",
        "get_database",
        "update_database_ddl",
        "drop_database",
        "get_database_ddl",
        "set_iam_policy",
        "get_iam_policy",
        "test_iam_permissions",
        "create_backup",
        "get_backup",
        "update_backup",
        "delete_backup",
        "list_backups",
        "restore_database",
        "list_database_operations",
        "list_backup_operations",
    )
    for method in methods:
        with pytest.raises(NotImplementedError):
            getattr(transport, method)(request=object())

    # Additionally, the LRO client (a property) should
    # also raise NotImplementedError
    with pytest.raises(NotImplementedError):
        transport.operations_client


@requires_google_auth_gte_1_25_0
def test_database_admin_base_transport_with_credentials_file():
    # Instantiate the base transport with a credentials file
    with mock.patch.object(
        google.auth, "load_credentials_from_file", autospec=True
    ) as load_creds, mock.patch(
        "google.cloud.spanner_admin_database_v1.services.database_admin.transports.DatabaseAdminTransport._prep_wrapped_messages"
    ) as Transport:
        Transport.return_value = None
        load_creds.return_value = (ga_credentials.AnonymousCredentials(), None)
        transport = transports.DatabaseAdminTransport(
            credentials_file="credentials.json", quota_project_id="octopus",
        )
        load_creds.assert_called_once_with(
            "credentials.json",
            scopes=None,
            default_scopes=(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/spanner.admin",
            ),
            quota_project_id="octopus",
        )


@requires_google_auth_lt_1_25_0
def test_database_admin_base_transport_with_credentials_file_old_google_auth():
    # Instantiate the base transport with a credentials file
    with mock.patch.object(
        google.auth, "load_credentials_from_file", autospec=True
    ) as load_creds, mock.patch(
        "google.cloud.spanner_admin_database_v1.services.database_admin.transports.DatabaseAdminTransport._prep_wrapped_messages"
    ) as Transport:
        Transport.return_value = None
        load_creds.return_value = (ga_credentials.AnonymousCredentials(), None)
        transport = transports.DatabaseAdminTransport(
            credentials_file="credentials.json", quota_project_id="octopus",
        )
        load_creds.assert_called_once_with(
            "credentials.json",
            scopes=(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/spanner.admin",
            ),
            quota_project_id="octopus",
        )


def test_database_admin_base_transport_with_adc():
    # Test the default credentials are used if credentials and credentials_file are None.
    with mock.patch.object(google.auth, "default", autospec=True) as adc, mock.patch(
        "google.cloud.spanner_admin_database_v1.services.database_admin.transports.DatabaseAdminTransport._prep_wrapped_messages"
    ) as Transport:
        Transport.return_value = None
        adc.return_value = (ga_credentials.AnonymousCredentials(), None)
        transport = transports.DatabaseAdminTransport()
        adc.assert_called_once()


@requires_google_auth_gte_1_25_0
def test_database_admin_auth_adc():
    # If no credentials are provided, we should use ADC credentials.
    with mock.patch.object(google.auth, "default", autospec=True) as adc:
        adc.return_value = (ga_credentials.AnonymousCredentials(), None)
        DatabaseAdminClient()
        adc.assert_called_once_with(
            scopes=None,
            default_scopes=(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/spanner.admin",
            ),
            quota_project_id=None,
        )


@requires_google_auth_lt_1_25_0
def test_database_admin_auth_adc_old_google_auth():
    # If no credentials are provided, we should use ADC credentials.
    with mock.patch.object(google.auth, "default", autospec=True) as adc:
        adc.return_value = (ga_credentials.AnonymousCredentials(), None)
        DatabaseAdminClient()
        adc.assert_called_once_with(
            scopes=(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/spanner.admin",
            ),
            quota_project_id=None,
        )


@pytest.mark.parametrize(
    "transport_class",
    [
        transports.DatabaseAdminGrpcTransport,
        transports.DatabaseAdminGrpcAsyncIOTransport,
    ],
)
@requires_google_auth_gte_1_25_0
def test_database_admin_transport_auth_adc(transport_class):
    # If credentials and host are not provided, the transport class should use
    # ADC credentials.
    with mock.patch.object(google.auth, "default", autospec=True) as adc:
        adc.return_value = (ga_credentials.AnonymousCredentials(), None)
        transport_class(quota_project_id="octopus", scopes=["1", "2"])
        adc.assert_called_once_with(
            scopes=["1", "2"],
            default_scopes=(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/spanner.admin",
            ),
            quota_project_id="octopus",
        )


@pytest.mark.parametrize(
    "transport_class",
    [
        transports.DatabaseAdminGrpcTransport,
        transports.DatabaseAdminGrpcAsyncIOTransport,
    ],
)
@requires_google_auth_lt_1_25_0
def test_database_admin_transport_auth_adc_old_google_auth(transport_class):
    # If credentials and host are not provided, the transport class should use
    # ADC credentials.
    with mock.patch.object(google.auth, "default", autospec=True) as adc:
        adc.return_value = (ga_credentials.AnonymousCredentials(), None)
        transport_class(quota_project_id="octopus")
        adc.assert_called_once_with(
            scopes=(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/spanner.admin",
            ),
            quota_project_id="octopus",
        )


@pytest.mark.parametrize(
    "transport_class,grpc_helpers",
    [
        (transports.DatabaseAdminGrpcTransport, grpc_helpers),
        (transports.DatabaseAdminGrpcAsyncIOTransport, grpc_helpers_async),
    ],
)
def test_database_admin_transport_create_channel(transport_class, grpc_helpers):
    # If credentials and host are not provided, the transport class should use
    # ADC credentials.
    with mock.patch.object(
        google.auth, "default", autospec=True
    ) as adc, mock.patch.object(
        grpc_helpers, "create_channel", autospec=True
    ) as create_channel:
        creds = ga_credentials.AnonymousCredentials()
        adc.return_value = (creds, None)
        transport_class(quota_project_id="octopus", scopes=["1", "2"])

        create_channel.assert_called_with(
            "spanner.googleapis.com:443",
            credentials=creds,
            credentials_file=None,
            quota_project_id="octopus",
            default_scopes=(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/spanner.admin",
            ),
            scopes=["1", "2"],
            default_host="spanner.googleapis.com",
            ssl_credentials=None,
            options=[
                ("grpc.max_send_message_length", -1),
                ("grpc.max_receive_message_length", -1),
            ],
        )


@pytest.mark.parametrize(
    "transport_class",
    [
        transports.DatabaseAdminGrpcTransport,
        transports.DatabaseAdminGrpcAsyncIOTransport,
    ],
)
def test_database_admin_grpc_transport_client_cert_source_for_mtls(transport_class):
    cred = ga_credentials.AnonymousCredentials()

    # Check ssl_channel_credentials is used if provided.
    with mock.patch.object(transport_class, "create_channel") as mock_create_channel:
        mock_ssl_channel_creds = mock.Mock()
        transport_class(
            host="squid.clam.whelk",
            credentials=cred,
            ssl_channel_credentials=mock_ssl_channel_creds,
        )
        mock_create_channel.assert_called_once_with(
            "squid.clam.whelk:443",
            credentials=cred,
            credentials_file=None,
            scopes=None,
            ssl_credentials=mock_ssl_channel_creds,
            quota_project_id=None,
            options=[
                ("grpc.max_send_message_length", -1),
                ("grpc.max_receive_message_length", -1),
            ],
        )

    # Check if ssl_channel_credentials is not provided, then client_cert_source_for_mtls
    # is used.
    with mock.patch.object(transport_class, "create_channel", return_value=mock.Mock()):
        with mock.patch("grpc.ssl_channel_credentials") as mock_ssl_cred:
            transport_class(
                credentials=cred,
                client_cert_source_for_mtls=client_cert_source_callback,
            )
            expected_cert, expected_key = client_cert_source_callback()
            mock_ssl_cred.assert_called_once_with(
                certificate_chain=expected_cert, private_key=expected_key
            )


def test_database_admin_host_no_port():
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(),
        client_options=client_options.ClientOptions(
            api_endpoint="spanner.googleapis.com"
        ),
    )
    assert client.transport._host == "spanner.googleapis.com:443"


def test_database_admin_host_with_port():
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(),
        client_options=client_options.ClientOptions(
            api_endpoint="spanner.googleapis.com:8000"
        ),
    )
    assert client.transport._host == "spanner.googleapis.com:8000"


def test_database_admin_grpc_transport_channel():
    channel = grpc.secure_channel("http://localhost/", grpc.local_channel_credentials())

    # Check that channel is used if provided.
    transport = transports.DatabaseAdminGrpcTransport(
        host="squid.clam.whelk", channel=channel,
    )
    assert transport.grpc_channel == channel
    assert transport._host == "squid.clam.whelk:443"
    assert transport._ssl_channel_credentials == None


def test_database_admin_grpc_asyncio_transport_channel():
    channel = aio.secure_channel("http://localhost/", grpc.local_channel_credentials())

    # Check that channel is used if provided.
    transport = transports.DatabaseAdminGrpcAsyncIOTransport(
        host="squid.clam.whelk", channel=channel,
    )
    assert transport.grpc_channel == channel
    assert transport._host == "squid.clam.whelk:443"
    assert transport._ssl_channel_credentials == None


# Remove this test when deprecated arguments (api_mtls_endpoint, client_cert_source) are
# removed from grpc/grpc_asyncio transport constructor.
@pytest.mark.parametrize(
    "transport_class",
    [
        transports.DatabaseAdminGrpcTransport,
        transports.DatabaseAdminGrpcAsyncIOTransport,
    ],
)
def test_database_admin_transport_channel_mtls_with_client_cert_source(transport_class):
    with mock.patch(
        "grpc.ssl_channel_credentials", autospec=True
    ) as grpc_ssl_channel_cred:
        with mock.patch.object(
            transport_class, "create_channel"
        ) as grpc_create_channel:
            mock_ssl_cred = mock.Mock()
            grpc_ssl_channel_cred.return_value = mock_ssl_cred

            mock_grpc_channel = mock.Mock()
            grpc_create_channel.return_value = mock_grpc_channel

            cred = ga_credentials.AnonymousCredentials()
            with pytest.warns(DeprecationWarning):
                with mock.patch.object(google.auth, "default") as adc:
                    adc.return_value = (cred, None)
                    transport = transport_class(
                        host="squid.clam.whelk",
                        api_mtls_endpoint="mtls.squid.clam.whelk",
                        client_cert_source=client_cert_source_callback,
                    )
                    adc.assert_called_once()

            grpc_ssl_channel_cred.assert_called_once_with(
                certificate_chain=b"cert bytes", private_key=b"key bytes"
            )
            grpc_create_channel.assert_called_once_with(
                "mtls.squid.clam.whelk:443",
                credentials=cred,
                credentials_file=None,
                scopes=None,
                ssl_credentials=mock_ssl_cred,
                quota_project_id=None,
                options=[
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                ],
            )
            assert transport.grpc_channel == mock_grpc_channel
            assert transport._ssl_channel_credentials == mock_ssl_cred


# Remove this test when deprecated arguments (api_mtls_endpoint, client_cert_source) are
# removed from grpc/grpc_asyncio transport constructor.
@pytest.mark.parametrize(
    "transport_class",
    [
        transports.DatabaseAdminGrpcTransport,
        transports.DatabaseAdminGrpcAsyncIOTransport,
    ],
)
def test_database_admin_transport_channel_mtls_with_adc(transport_class):
    mock_ssl_cred = mock.Mock()
    with mock.patch.multiple(
        "google.auth.transport.grpc.SslCredentials",
        __init__=mock.Mock(return_value=None),
        ssl_credentials=mock.PropertyMock(return_value=mock_ssl_cred),
    ):
        with mock.patch.object(
            transport_class, "create_channel"
        ) as grpc_create_channel:
            mock_grpc_channel = mock.Mock()
            grpc_create_channel.return_value = mock_grpc_channel
            mock_cred = mock.Mock()

            with pytest.warns(DeprecationWarning):
                transport = transport_class(
                    host="squid.clam.whelk",
                    credentials=mock_cred,
                    api_mtls_endpoint="mtls.squid.clam.whelk",
                    client_cert_source=None,
                )

            grpc_create_channel.assert_called_once_with(
                "mtls.squid.clam.whelk:443",
                credentials=mock_cred,
                credentials_file=None,
                scopes=None,
                ssl_credentials=mock_ssl_cred,
                quota_project_id=None,
                options=[
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                ],
            )
            assert transport.grpc_channel == mock_grpc_channel


def test_database_admin_grpc_lro_client():
    client = DatabaseAdminClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc",
    )
    transport = client.transport

    # Ensure that we have a api-core operations client.
    assert isinstance(transport.operations_client, operations_v1.OperationsClient,)

    # Ensure that subsequent calls to the property send the exact same object.
    assert transport.operations_client is transport.operations_client


def test_database_admin_grpc_lro_async_client():
    client = DatabaseAdminAsyncClient(
        credentials=ga_credentials.AnonymousCredentials(), transport="grpc_asyncio",
    )
    transport = client.transport

    # Ensure that we have a api-core operations client.
    assert isinstance(transport.operations_client, operations_v1.OperationsAsyncClient,)

    # Ensure that subsequent calls to the property send the exact same object.
    assert transport.operations_client is transport.operations_client


def test_backup_path():
    project = "squid"
    instance = "clam"
    backup = "whelk"
    expected = "projects/{project}/instances/{instance}/backups/{backup}".format(
        project=project, instance=instance, backup=backup,
    )
    actual = DatabaseAdminClient.backup_path(project, instance, backup)
    assert expected == actual


def test_parse_backup_path():
    expected = {
        "project": "octopus",
        "instance": "oyster",
        "backup": "nudibranch",
    }
    path = DatabaseAdminClient.backup_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_backup_path(path)
    assert expected == actual


def test_crypto_key_path():
    project = "cuttlefish"
    location = "mussel"
    key_ring = "winkle"
    crypto_key = "nautilus"
    expected = "projects/{project}/locations/{location}/keyRings/{key_ring}/cryptoKeys/{crypto_key}".format(
        project=project, location=location, key_ring=key_ring, crypto_key=crypto_key,
    )
    actual = DatabaseAdminClient.crypto_key_path(
        project, location, key_ring, crypto_key
    )
    assert expected == actual


def test_parse_crypto_key_path():
    expected = {
        "project": "scallop",
        "location": "abalone",
        "key_ring": "squid",
        "crypto_key": "clam",
    }
    path = DatabaseAdminClient.crypto_key_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_crypto_key_path(path)
    assert expected == actual


def test_crypto_key_version_path():
    project = "whelk"
    location = "octopus"
    key_ring = "oyster"
    crypto_key = "nudibranch"
    crypto_key_version = "cuttlefish"
    expected = "projects/{project}/locations/{location}/keyRings/{key_ring}/cryptoKeys/{crypto_key}/cryptoKeyVersions/{crypto_key_version}".format(
        project=project,
        location=location,
        key_ring=key_ring,
        crypto_key=crypto_key,
        crypto_key_version=crypto_key_version,
    )
    actual = DatabaseAdminClient.crypto_key_version_path(
        project, location, key_ring, crypto_key, crypto_key_version
    )
    assert expected == actual


def test_parse_crypto_key_version_path():
    expected = {
        "project": "mussel",
        "location": "winkle",
        "key_ring": "nautilus",
        "crypto_key": "scallop",
        "crypto_key_version": "abalone",
    }
    path = DatabaseAdminClient.crypto_key_version_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_crypto_key_version_path(path)
    assert expected == actual


def test_database_path():
    project = "squid"
    instance = "clam"
    database = "whelk"
    expected = "projects/{project}/instances/{instance}/databases/{database}".format(
        project=project, instance=instance, database=database,
    )
    actual = DatabaseAdminClient.database_path(project, instance, database)
    assert expected == actual


def test_parse_database_path():
    expected = {
        "project": "octopus",
        "instance": "oyster",
        "database": "nudibranch",
    }
    path = DatabaseAdminClient.database_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_database_path(path)
    assert expected == actual


def test_instance_path():
    project = "cuttlefish"
    instance = "mussel"
    expected = "projects/{project}/instances/{instance}".format(
        project=project, instance=instance,
    )
    actual = DatabaseAdminClient.instance_path(project, instance)
    assert expected == actual


def test_parse_instance_path():
    expected = {
        "project": "winkle",
        "instance": "nautilus",
    }
    path = DatabaseAdminClient.instance_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_instance_path(path)
    assert expected == actual


def test_common_billing_account_path():
    billing_account = "scallop"
    expected = "billingAccounts/{billing_account}".format(
        billing_account=billing_account,
    )
    actual = DatabaseAdminClient.common_billing_account_path(billing_account)
    assert expected == actual


def test_parse_common_billing_account_path():
    expected = {
        "billing_account": "abalone",
    }
    path = DatabaseAdminClient.common_billing_account_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_common_billing_account_path(path)
    assert expected == actual


def test_common_folder_path():
    folder = "squid"
    expected = "folders/{folder}".format(folder=folder,)
    actual = DatabaseAdminClient.common_folder_path(folder)
    assert expected == actual


def test_parse_common_folder_path():
    expected = {
        "folder": "clam",
    }
    path = DatabaseAdminClient.common_folder_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_common_folder_path(path)
    assert expected == actual


def test_common_organization_path():
    organization = "whelk"
    expected = "organizations/{organization}".format(organization=organization,)
    actual = DatabaseAdminClient.common_organization_path(organization)
    assert expected == actual


def test_parse_common_organization_path():
    expected = {
        "organization": "octopus",
    }
    path = DatabaseAdminClient.common_organization_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_common_organization_path(path)
    assert expected == actual


def test_common_project_path():
    project = "oyster"
    expected = "projects/{project}".format(project=project,)
    actual = DatabaseAdminClient.common_project_path(project)
    assert expected == actual


def test_parse_common_project_path():
    expected = {
        "project": "nudibranch",
    }
    path = DatabaseAdminClient.common_project_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_common_project_path(path)
    assert expected == actual


def test_common_location_path():
    project = "cuttlefish"
    location = "mussel"
    expected = "projects/{project}/locations/{location}".format(
        project=project, location=location,
    )
    actual = DatabaseAdminClient.common_location_path(project, location)
    assert expected == actual


def test_parse_common_location_path():
    expected = {
        "project": "winkle",
        "location": "nautilus",
    }
    path = DatabaseAdminClient.common_location_path(**expected)

    # Check that the path construction is reversible.
    actual = DatabaseAdminClient.parse_common_location_path(path)
    assert expected == actual


def test_client_withDEFAULT_CLIENT_INFO():
    client_info = gapic_v1.client_info.ClientInfo()

    with mock.patch.object(
        transports.DatabaseAdminTransport, "_prep_wrapped_messages"
    ) as prep:
        client = DatabaseAdminClient(
            credentials=ga_credentials.AnonymousCredentials(), client_info=client_info,
        )
        prep.assert_called_once_with(client_info)

    with mock.patch.object(
        transports.DatabaseAdminTransport, "_prep_wrapped_messages"
    ) as prep:
        transport_class = DatabaseAdminClient.get_transport_class()
        transport = transport_class(
            credentials=ga_credentials.AnonymousCredentials(), client_info=client_info,
        )
        prep.assert_called_once_with(client_info)
