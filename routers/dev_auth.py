"""Development authentication routes — local-only mock auth.

Ported from: backend/chalicelib/blueprints/dev_auth.py

SECURITY: Only active when LOCAL_INFRA=1.
"""

from fastapi import APIRouter, Body

from chalicelib.controllers.user import UserController
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.local_auth import create_mock_user_tokens
from chalicelib.new.utils.session import new_session
from chalicelib.schema.models.user import User
from exceptions import ForbiddenError

router = APIRouter(tags=["DevAuth"])


def _ensure_local_mode():
    """Allow /dev/* when LOCAL_INFRA=1 or API base URL is localhost (same machine as Vite)."""
    if not envars.LOCAL_DEV_API:
        raise ForbiddenError(
            "Development auth endpoints are only available for local development "
            "(LOCAL_INFRA=1 or VITE_REACT_APP_BASE_URL pointing at localhost). "
            "They are disabled when the API is configured for a remote host."
        )


@router.post("/dev/login")
def dev_login(body: dict = Body(...)):
    _ensure_local_mode()

    email = body.get("email")
    if not email:
        raise ForbiddenError("Email is required")

    with new_session(comment="dev_login", read_only=False) as session:
        cognito_sub = f"local-{email.replace('@', '-').replace('.', '-')}"
        user = UserController.get_or_create_from_email(email, session)
        user.cognito_sub = cognito_sub
        session.commit()

        tokens = create_mock_user_tokens(user.email)
        user_info = UserController.get_basic_info(user, session=session)

        return {**tokens, "user": user_info}


@router.post("/dev/token")
def dev_generate_token(body: dict = Body(...)):
    _ensure_local_mode()

    email = body.get("email")
    if not email:
        raise ForbiddenError("Email is required")

    with new_session(comment="dev_token", read_only=True) as session:
        user = UserController.get_user_by_email(email, session=session)
        if not user:
            raise ForbiddenError(
                f"User with email {email} not found. Use /dev/login to create a new user."
            )
        return create_mock_user_tokens(user.email)


@router.get("/dev/users")
def dev_list_users():
    _ensure_local_mode()

    with new_session(comment="dev_users", read_only=True) as session:
        users = session.query(User).limit(100).all()
        return {
            "users": [
                {
                    "id": u.id,
                    "email": u.email,
                    "name": u.name,
                    "cognito_sub": u.cognito_sub,
                    "identifier": str(u.identifier),
                }
                for u in users
            ],
            "total": len(users),
        }


@router.get("/dev/auth-status")
def dev_auth_status():
    _ensure_local_mode()

    return {
        "local_mode": True,
        "cognito_mocked": True,
        "local_infra": envars.LOCAL_INFRA,
        "cognito_client_id": envars.COGNITO_CLIENT_ID,
        "cognito_user_pool_id": envars.COGNITO_USER_POOL_ID,
        "message": (
            "Running in local development mode with mocked authentication. "
            "JWT signatures are not verified. Use /dev/login to get test tokens."
        ),
    }
