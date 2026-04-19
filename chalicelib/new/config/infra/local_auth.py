"""
Local JWT (PyJWT, HS256) for AUTH_BACKEND=local_jwt — no Cognito at runtime.

Issues Cognito-shaped **AuthenticationResult** keys (IdToken, AccessToken, RefreshToken).
Validates issuer (**JWT_ISS**), audience (**JWT_AUD**), and **JWT_SECRET** on decode.

SECURITY: With **LOCAL_INFRA=0** (e.g. Azure ACA), **JWT_SECRET** must be set (**envars**);
with **LOCAL_INFRA=1**, a dev-only default secret applies if **JWT_SECRET** is unset.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

import bcrypt
import jwt

# Default HS256 secret when JWT_SECRET is unset (LOCAL_INFRA dev only)
LOCAL_SECRET_KEY = "local-dev-secret-key-do-not-use-in-production-this-is-only-for-testing"

# Access / ID token lifetime (seconds); Cognito-shaped ExpiresIn
_DEFAULT_EXPIRES_IN = 86400
_REFRESH_DAYS = 30


def _jwt_secret() -> str:
    return os.environ.get("JWT_SECRET", LOCAL_SECRET_KEY)


def _jwt_iss() -> str:
    return os.environ.get("JWT_ISS", "http://localhost:4566")


def _jwt_aud() -> str:
    return os.environ.get("JWT_AUD", "local_mock_client")


def _now_ts() -> int:
    return int(datetime.now(UTC).timestamp())


def _encode_hs256(payload: dict[str, Any]) -> str:
    return jwt.encode(payload, _jwt_secret(), algorithm="HS256")


def hash_password(plain_password: str) -> str:
    """Hash a password with bcrypt for storage in ``user.password_hash``."""
    pw = plain_password.encode("utf-8")
    return bcrypt.hashpw(pw, bcrypt.gensalt()).decode("utf-8")


def verify_password(plain_password: str, password_hash: str) -> bool:
    """Verify a plaintext password against a bcrypt hash string."""
    if not password_hash:
        return False
    try:
        return bcrypt.checkpw(
            plain_password.encode("utf-8"),
            password_hash.encode("utf-8"),
        )
    except ValueError:
        return False


def create_local_jwt(
    email: str,
    cognito_sub: str,
    expires_in_hours: int = 24,
) -> str:
    """
    Create a single ID-shaped JWT (legacy helper; prefer ``issue_authentication_result``).

    Claims: **sub**, **email**, **exp**, **iat**, and **iss** / **aud** / **token_use**.
    """
    now = _now_ts()
    exp = now + int(timedelta(hours=expires_in_hours).total_seconds())
    payload = {
        "sub": cognito_sub,
        "email": email,
        "iss": _jwt_iss(),
        "aud": _jwt_aud(),
        "iat": now,
        "exp": exp,
        "token_use": "id",
    }
    return _encode_hs256(payload)


def decode_local_jwt(token: str, verify_signature: bool = True) -> dict[str, Any]:
    """
    Decode and validate an **access** or **id** token (API ``access_token`` header).

    Rejects **refresh** tokens so they cannot be used as session tokens.

    Args:
        token: JWT string
        verify_signature: If False, decode only (not used in production paths)

    Returns:
        Decoded payload

    Raises:
        jwt.PyJWTError: Invalid or expired token
    """
    if not verify_signature:
        decoded = jwt.decode(
            token,
            options={
                "verify_signature": False,
                "verify_exp": False,
                "verify_aud": False,
            },
        )
    else:
        decoded = jwt.decode(
            token,
            _jwt_secret(),
            algorithms=["HS256"],
            audience=_jwt_aud(),
            issuer=_jwt_iss(),
            options={
                "verify_signature": True,
                "verify_exp": True,
                "verify_aud": True,
            },
        )
    tu = decoded.get("token_use")
    if tu not in ("id", "access"):
        raise jwt.InvalidTokenError("Expected id or access token")
    if "sub" not in decoded:
        raise jwt.InvalidTokenError("Missing sub claim")
    if verify_signature and "email" not in decoded:
        raise jwt.InvalidTokenError("Missing email claim")
    return decoded


def decode_refresh_token(token: str) -> dict[str, Any]:
    """Validate a refresh JWT issued by ``issue_authentication_result``."""
    payload = jwt.decode(
        token,
        _jwt_secret(),
        algorithms=["HS256"],
        audience=_jwt_aud(),
        issuer=_jwt_iss(),
        options={"verify_signature": True, "verify_exp": True, "verify_aud": True},
    )
    if payload.get("token_use") != "refresh":
        raise jwt.InvalidTokenError("Not a refresh token")
    if "sub" not in payload:
        raise jwt.InvalidTokenError("Missing sub claim")
    return payload


def decode_token_without_verification(token: str) -> dict[str, Any]:
    """
    Decode any JWT without signature verification (dev / introspection only).
    """
    return jwt.decode(
        token,
        options={"verify_signature": False, "verify_exp": False, "verify_aud": False},
    )


def issue_authentication_result(email: str, cognito_sub: str) -> dict[str, Any]:
    """
    Build a Cognito-shaped AuthenticationResult for the frontend (PascalCase keys).

    Id/Access: **sub**, **email**, **exp**, **iat**, **iss**, **aud**, **token_use**.
    Refresh: **sub**, **exp**, **iat**, **iss**, **aud**, **token_use** (no **email**).
    """
    now = _now_ts()
    exp_access = now + _DEFAULT_EXPIRES_IN
    exp_refresh = now + int(timedelta(days=_REFRESH_DAYS).total_seconds())
    iss = _jwt_iss()
    aud = _jwt_aud()

    def _id_access_claims(token_use: str) -> dict[str, Any]:
        return {
            "sub": cognito_sub,
            "email": email,
            "iss": iss,
            "aud": aud,
            "iat": now,
            "exp": exp_access,
            "token_use": token_use,
        }

    id_token = _encode_hs256(_id_access_claims("id"))
    access_token = _encode_hs256(_id_access_claims("access"))
    refresh_token = _encode_hs256(
        {
            "sub": cognito_sub,
            "iss": iss,
            "aud": aud,
            "iat": now,
            "exp": exp_refresh,
            "token_use": "refresh",
        }
    )

    return {
        "AccessToken": access_token,
        "ExpiresIn": _DEFAULT_EXPIRES_IN,
        "TokenType": "Bearer",
        "RefreshToken": refresh_token,
        "IdToken": id_token,
    }


def create_mock_user_tokens(email: str) -> dict[str, str]:
    """
    Create mock tokens for dev routes (camelCase keys).
    """
    cognito_sub = f"local-{email.replace('@', '-').replace('.', '-')}"
    ar = issue_authentication_result(email, cognito_sub)
    return {
        "idToken": ar["IdToken"],
        "accessToken": ar["AccessToken"],
        "refreshToken": ar["RefreshToken"],
        "expiresIn": ar["ExpiresIn"],
        "tokenType": ar["TokenType"],
    }
