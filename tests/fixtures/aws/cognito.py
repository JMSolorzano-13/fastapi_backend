import json
from collections.abc import Generator
from contextlib import contextmanager

import pytest
from app import app
from boto3_type_annotations.cognito_idp import Client as CognitoClient
from chalice.test import Client, HTTPResponse, TestHTTPClient
from moto import mock_aws

from chalicelib.infra.localstack_boto_clients import make_ephemeral_cognito_client
from chalicelib.new.config.infra import envars


class TestHTTPClientWithHeaders(TestHTTPClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_headers = {}

    def request(
        self, method: str, path: str, headers: dict[str, str] | None = None, body: bytes = b""
    ) -> HTTPResponse:
        combined_headers = self.default_headers | (headers or {})
        if method.upper() in ("GET", "DELETE"):
            combined_headers.pop("Content-Type", None)
        if type(body) is dict:  # QoL para no tener que hacer json.dumps en cada request
            body = json.dumps(body).encode("utf-8")
        return super().request(method, path, headers=combined_headers, body=body)


class ClientWithHeaders(Client):
    @property
    def http(self) -> TestHTTPClientWithHeaders:
        if self._http_client is None:
            self._http_client = TestHTTPClientWithHeaders(self._app, self._chalice_config)
        return self._http_client  # type: ignore


@pytest.fixture
def client() -> Generator[Client, None, None]:
    with ClientWithHeaders(app) as client:
        yield client


@pytest.fixture(autouse=True)
def mock_session(mocker, session, commit_session):
    if not commit_session:
        mocker.patch("chalicelib.schema.get_engine", return_value=session.bind)

        @contextmanager
        def mock_new_session(*args, **kwargs):
            yield session

        mocker.patch("chalicelib.new.utils.session.new_session", mock_new_session)
        mocker.patch("chalicelib.new.utils.session._new_session", mock_new_session)

        mocker.patch("chalicelib.blueprints.superblueprint.new_session", mock_new_session)
    yield


@pytest.fixture()
def cognito() -> Generator[CognitoClient, None, None]:
    with mock_aws():
        client = make_ephemeral_cognito_client(region_name=envars.REGION_NAME)
        yield client


@pytest.fixture(autouse=True)
def cognito_pool_and_client(cognito: CognitoClient):
    user_pool = cognito.create_user_pool(PoolName="test")
    envars.COGNITO_USER_POOL_ID = user_pool["UserPool"]["Id"]

    client = cognito.create_user_pool_client(
        UserPoolId=envars.COGNITO_USER_POOL_ID,
        ClientName="test",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
    )
    envars.COGNITO_CLIENT_ID = client["UserPoolClient"]["ClientId"]


@pytest.fixture(autouse=True)  # Cambiado a False para permitir tokens reales
def mock_get_signing_key(mocker):
    mocker.patch("chalicelib.controllers.cognito.get_signing_key", return_value="")
    mocker.patch(
        "chalicelib.controllers.cognito.get_options",
        return_value={"verify_signature": False},
    )
