"""
Development Authentication Blueprint

Provides mock authentication endpoints for local development when Cognito is not available.

SECURITY WARNING: These endpoints should ONLY be available when LOCAL_INFRA=1.
They bypass all security checks and should never be exposed in production.
"""

from chalice import Blueprint, ForbiddenError
from sqlalchemy.orm import Session

from chalicelib.controllers.user import UserController
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.local_auth import create_mock_user_tokens
from chalicelib.new.utils.session import new_session

blueprint = Blueprint(__name__)


def _ensure_local_mode():
    """Ensure these endpoints only work in local development mode."""
    if not envars.LOCAL_INFRA:
        raise ForbiddenError(
            "Development auth endpoints are only available when LOCAL_INFRA=1. "
            "These endpoints are disabled in production for security."
        )


@blueprint.route("/dev/login", methods=["POST"], cors=True)
def dev_login():
    """
    Mock login endpoint for local development.

    Creates or retrieves a user by email and returns mock JWT tokens.

    Request Body:
        {
            "email": "user@example.com"
        }

    Response:
        {
            "idToken": "eyJ...",
            "accessToken": "eyJ...",
            "refreshToken": "mock-refresh-...",
            "expiresIn": 86400,
            "tokenType": "Bearer",
            "user": {
                "id": 1,
                "email": "user@example.com",
                "cognito_sub": "local-user-example-com",
                ...
            }
        }

    Example:
        curl -X POST http://localhost:8000/api/dev/login \\
             -H "Content-Type: application/json" \\
             -d '{"email": "dev@local.test"}'
    """
    _ensure_local_mode()

    body = blueprint.current_request.json_body
    email = body.get("email")

    if not email:
        raise ForbiddenError("Email is required")

    with new_session(comment="dev_login", read_only=False) as session:
        # Create cognito_sub that matches what will be in the JWT
        cognito_sub = f"local-{email.replace('@', '-').replace('.', '-')}"
        
        # Get or create user
        user = UserController.get_or_create_from_email(email, session)
        
        # Update user's cognito_sub to match the JWT token
        # This ensures get_by_token() will find the user
        user.cognito_sub = cognito_sub
        
        session.commit()

        # Generate mock tokens
        tokens = create_mock_user_tokens(user.email)

        # Get user info
        user_info = UserController.get_basic_info(user, session=session)

        return {
            **tokens,
            "user": user_info,
        }


@blueprint.route("/dev/token", methods=["POST"], cors=True)
def dev_generate_token():
    """
    Generate a token for an existing user by email.

    Unlike /dev/login, this doesn't create users, only generates tokens.

    Request Body:
        {
            "email": "existing@example.com"
        }

    Response:
        {
            "idToken": "eyJ...",
            "accessToken": "eyJ...",
            "refreshToken": "mock-refresh-...",
            "expiresIn": 86400,
            "tokenType": "Bearer"
        }

    Example:
        curl -X POST http://localhost:8000/api/dev/token \\
             -H "Content-Type: application/json" \\
             -d '{"email": "existing@local.test"}'
    """
    _ensure_local_mode()

    body = blueprint.current_request.json_body
    email = body.get("email")

    if not email:
        raise ForbiddenError("Email is required")

    with new_session(comment="dev_token", read_only=True) as session:
        # Get existing user
        user = UserController.get_user_by_email(email, session)

        if not user:
            raise ForbiddenError(
                f"User with email {email} not found. Use /dev/login to create a new user."
            )

        # Generate mock tokens
        return create_mock_user_tokens(user.email)


@blueprint.route("/dev/users", methods=["GET"], cors=True)
def dev_list_users():
    """
    List all users in the database (for development/testing).

    Response:
        {
            "users": [
                {
                    "id": 1,
                    "email": "user1@example.com",
                    "cognito_sub": "local-user1-...",
                    ...
                },
                ...
            ],
            "total": 10
        }

    Example:
        curl http://localhost:8000/api/dev/users
    """
    _ensure_local_mode()

    with new_session(comment="dev_users", read_only=True) as session:
        from chalicelib.schema.models.user import User

        users = session.query(User).limit(100).all()

        return {
            "users": [
                {
                    "id": user.id,
                    "email": user.email,
                    "name": user.name,
                    "cognito_sub": user.cognito_sub,
                    "identifier": str(user.identifier),
                }
                for user in users
            ],
            "total": len(users),
        }


@blueprint.route("/dev/auth-status", methods=["GET"], cors=True)
def dev_auth_status():
    """
    Get authentication configuration status.

    Response:
        {
            "local_mode": true,
            "cognito_mocked": true,
            "message": "Running in local development mode..."
        }

    Example:
        curl http://localhost:8000/api/dev/auth-status
    """
    _ensure_local_mode()

    return {
        "local_mode": True,
        "cognito_mocked": True,
        "local_infra": envars.LOCAL_INFRA,
        "cognito_client_id": envars.COGNITO_CLIENT_ID,
        "cognito_user_pool_id": envars.COGNITO_USER_POOL_ID,
        "message": "Running in local development mode with mocked authentication. "
        "JWT signatures are not verified. Use /dev/login to get test tokens.",
    }
