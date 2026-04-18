"""
Local Authentication Module for Development

This module provides JWT token generation and validation for local development
when Cognito is not available (LocalStack Community Edition limitation).

SECURITY WARNING: This module should ONLY be used when LOCAL_INFRA=1.
It bypasses all Cognito security checks and should never be used in production.
"""

import jwt
from datetime import datetime, timedelta
from typing import Any

# Local development secret key (NOT FOR PRODUCTION)
LOCAL_SECRET_KEY = "local-dev-secret-key-do-not-use-in-production-this-is-only-for-testing"


def create_local_jwt(
    email: str,
    cognito_sub: str,
    expires_in_hours: int = 24,
) -> str:
    """
    Create a JWT token for local development.

    Args:
        email: User email address
        cognito_sub: Cognito subject (user identifier)
        expires_in_hours: Token expiration time in hours

    Returns:
        JWT token string

    Example:
        token = create_local_jwt("dev@local.test", "local-user-123")
    """
    now = datetime.utcnow()
    payload = {
        "sub": cognito_sub,
        "name": email,
        "email": email,
        "aud": "local_mock_client",
        "iss": "http://localhost:4566",
        "exp": now + timedelta(hours=expires_in_hours),
        "iat": now,
        "token_use": "id",
        "auth_time": int(now.timestamp()),
    }
    return jwt.encode(payload, LOCAL_SECRET_KEY, algorithm="HS256")


def decode_local_jwt(token: str, verify_signature: bool = True) -> dict[str, Any]:
    """
    Decode a local JWT token.

    Args:
        token: JWT token string
        verify_signature: Whether to verify the signature (default: True)

    Returns:
        Decoded token payload

    Raises:
        jwt.ExpiredSignatureError: If token is expired
        jwt.InvalidTokenError: If token is invalid
    """
    options = {
        "verify_signature": verify_signature,
        "verify_exp": True,
        "verify_aud": False,  # Don't verify audience for local tokens
    }

    return jwt.decode(
        token,
        LOCAL_SECRET_KEY,
        algorithms=["HS256"],
        options=options,
    )


def decode_token_without_verification(token: str) -> dict[str, Any]:
    """
    Decode any JWT token without signature verification.

    This is useful for extracting claims from tokens generated elsewhere
    (e.g., from a dev/staging Cognito instance) without needing to validate them.

    Args:
        token: JWT token string

    Returns:
        Decoded token payload

    Example:
        payload = decode_token_without_verification(real_cognito_token)
        email = payload["email"]
        cognito_sub = payload["sub"]
    """
    return jwt.decode(
        token,
        options={"verify_signature": False, "verify_exp": False, "verify_aud": False},
    )


def create_mock_user_tokens(email: str) -> dict[str, str]:
    """
    Create a complete set of mock tokens for a user.

    Args:
        email: User email address

    Returns:
        Dictionary with idToken, accessToken, and refreshToken

    Example:
        tokens = create_mock_user_tokens("dev@local.test")
        id_token = tokens["idToken"]
    """
    # Use email as cognito_sub for simplicity in local dev
    cognito_sub = f"local-{email.replace('@', '-').replace('.', '-')}"

    id_token = create_local_jwt(email, cognito_sub)

    return {
        "idToken": id_token,
        "accessToken": id_token,  # Same token for simplicity
        "refreshToken": f"mock-refresh-{cognito_sub}",
        "expiresIn": 86400,  # 24 hours in seconds
        "tokenType": "Bearer",
    }
