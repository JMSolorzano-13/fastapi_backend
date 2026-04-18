from http import HTTPStatus
from unittest.mock import patch

from chalice.test import Client
from sqlalchemy.orm import Session

from chalicelib.schema.models.user import User


def test_set_email_forbidden_for_non_admin(client_authenticated: Client):
    result = client_authenticated.http.put(
        "/User/set_email/target@test.com/new@test.com",
    )
    assert result.status_code == HTTPStatus.FORBIDDEN


def test_set_email_user_not_found(client_authenticated: Client):
    with patch("chalicelib.blueprints.superblueprint.assert_admin_user"):
        result = client_authenticated.http.put(
            "/User/set_email/nonexistent@test.com/new@test.com",
        )
    assert result.status_code == HTTPStatus.NOT_FOUND


def test_set_email_invalid_new_email(client_authenticated: Client):
    with patch("chalicelib.blueprints.superblueprint.assert_admin_user"):
        result = client_authenticated.http.put(
            "/User/set_email/old@test.com/not-an-email",
        )
    assert result.status_code == HTTPStatus.BAD_REQUEST


def test_set_email_duplicate_email(client_authenticated: Client, session: Session):
    existing = session.query(User).first()
    assert existing is not None

    with patch("chalicelib.blueprints.superblueprint.assert_admin_user"):
        result = client_authenticated.http.put(
            f"/User/set_email/other@test.com/{existing.email}",
        )
    assert result.status_code == HTTPStatus.BAD_REQUEST


def test_set_email_success(client_authenticated: Client, session: Session):
    target = session.query(User).filter(User.email == "user@test.com").first()
    assert target is not None

    with patch("chalicelib.blueprints.superblueprint.assert_admin_user"):
        result = client_authenticated.http.put(
            "/User/set_email/user@test.com/updated@test.com",
        )
    assert result.status_code == HTTPStatus.OK

    session.flush()
    session.refresh(target)
    assert target.email == "updated@test.com"
