"""UserController.auth when AUTH_BACKEND is patched to local_jwt (no real DB)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import chalicelib.controllers.user as user_controller_mod
from chalicelib.controllers.user import UserController
from chalicelib.new.config.infra import local_auth
from exceptions import UnauthorizedError


@pytest.fixture
def auth_as_local_jwt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JWT_SECRET", "pytest-local-jwt-secret-key-32chars-minimum-xx")
    monkeypatch.setattr(
        user_controller_mod.envars,
        "AUTH_BACKEND",
        "local_jwt",
        raising=False,
    )


def _session_finding_user(user: object) -> MagicMock:
    session = MagicMock()
    chain = MagicMock()
    session.query.return_value = chain
    chain.filter.return_value = chain
    chain.first.return_value = user
    chain.one.return_value = user
    return session


def test_local_jwt_user_password_auth_returns_pascal_case_tokens(
    auth_as_local_jwt: None,
) -> None:
    user = MagicMock()
    user.email = "LocalJwt_User@Test.Com"
    user.password_hash = local_auth.hash_password("Test123!")
    user.cognito_sub = "11111111-1111-1111-1111-111111111111"

    session = _session_finding_user(user)
    result = UserController.auth(
        "USER_PASSWORD_AUTH",
        {"USERNAME": user.email.lower(), "PASSWORD": "Test123!"},
        session=session,
    )

    expected_keys = {"IdToken", "AccessToken", "RefreshToken", "ExpiresIn", "TokenType"}
    assert expected_keys <= result.keys()
    assert result["TokenType"] == "Bearer"
    assert isinstance(result["ExpiresIn"], int)
    local_auth.decode_local_jwt(result["IdToken"])
    local_auth.decode_local_jwt(result["AccessToken"])
    local_auth.decode_refresh_token(result["RefreshToken"])


def test_local_jwt_refresh_token_auth(auth_as_local_jwt: None) -> None:
    user = MagicMock()
    user.email = "refresh@example.com"
    user.password_hash = local_auth.hash_password("Test123!")
    user.cognito_sub = "22222222-2222-2222-2222-222222222222"

    session = _session_finding_user(user)
    first = UserController.auth(
        "USER_PASSWORD_AUTH",
        {"USERNAME": user.email, "PASSWORD": "Test123!"},
        session=session,
    )
    second = UserController.auth(
        "REFRESH_TOKEN_AUTH",
        {"REFRESH_TOKEN": first["RefreshToken"]},
        session=session,
    )

    assert {"IdToken", "AccessToken", "RefreshToken"} <= second.keys()
    local_auth.decode_local_jwt(second["IdToken"])


def test_local_jwt_wrong_password(auth_as_local_jwt: None) -> None:
    user = MagicMock()
    user.email = "bad@example.com"
    user.password_hash = local_auth.hash_password("right")
    user.cognito_sub = "33333333-3333-3333-3333-333333333333"

    session = _session_finding_user(user)
    with pytest.raises(UnauthorizedError):
        UserController.auth(
            "USER_PASSWORD_AUTH",
            {"USERNAME": user.email, "PASSWORD": "wrong"},
            session=session,
        )
