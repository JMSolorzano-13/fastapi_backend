import functools
import inspect
import uuid
from collections.abc import Callable
from typing import Any

from chalice import Blueprint, ForbiddenError, UnauthorizedError  # type: ignore
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from chalicelib.controllers.permission import Role
from chalicelib.controllers.tenant.session import new_company_session
from chalicelib.controllers.tenant.utils import tenant_url_from_identifier
from chalicelib.controllers.user import UserController
from chalicelib.new.config.infra import envars
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.session import new_session
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.permission import Permission
from chalicelib.schema.models.user import User
from chalicelib.tenant_utils import (
    check_body,
    check_domain,
    check_header,
    check_uri_params,
)

dep_chain: dict[str, set[str]] = {
    "user": {"session"},
    "user_identifier": {"user"},
    "user_id": {"user"},
    "company_identifier": {"session", "user"},
    "company_session": {"company_identifier"},
    "company": {"company_identifier"},
    "admin_create_user": {"session", "user"},
    "admin_user": {"session", "user"},
}


def resolve_dependencies(parameters):
    resolved: set[str] = set()
    unresolved: set[str] = set(parameters)
    while unresolved:
        current = unresolved.pop()
        sub_deps: set[str] = dep_chain.get(current, set())
        unresolved |= sub_deps - resolved
        resolved.add(current)
    return resolved


def assert_user_can_access_company(
    user: User,
    company: Company,  # TODO: use Identifier
    session: Session,
    role: Role = Role.OPERATOR,
) -> None:
    """Checks if the user has access to the company with the given identifier.

    Raises an UnauthorizedError if the user does not have access."""
    if not session.execute(
        select(func.count())
        .select_from(Permission)
        .where(
            Permission.user_id == user.id,
            Permission.company_id == company.id,
            Permission.role == role.name,
        )
    ).scalar():
        raise UnauthorizedError("No company found")


def assert_admin_user(user: User) -> None:
    if user.email not in envars.control.ADMIN_EMAILS:
        raise ForbiddenError("Only admin users can perform this action")


def assert_user_can_admin_create_company(user: User) -> None:
    if user.email in envars.control.ADMIN_EMAILS:
        return
    raise UnauthorizedError(
        "User does not have permission to create a company. "
        "Please contact support if you believe this is an error."
    )


class SuperBlueprint(Blueprint):
    def get_company_identifier_from_request(self) -> Identifier:
        """Tries to get the company identifier from the request body, domain or header.

        If not found, it raises an UnauthorizedError."""

        json_body = self.current_request.json_body or {}
        headers = self.current_request.headers

        company_identifier = (
            check_body(json_body)
            or check_domain(json_body)
            or check_header(headers)
            or check_uri_params(self.current_request.uri_params or {})
        )
        if not company_identifier:
            raise UnauthorizedError("No company identifier provided")
        return Identifier(uuid.UUID(company_identifier))

    def _inject_user(self, dependencies_dict: dict[str, Any]) -> None:
        session = dependencies_dict["session"]
        token = self.current_request.headers["access_token"]
        user = UserController.get_by_token(token, session=session)
        dependencies_dict.setdefault("user", user)
        dependencies_dict.setdefault("user_identifier", user.identifier)
        dependencies_dict.setdefault("user_id", user.id)

    def _inject_admin_create_user(self, dependencies_dict: dict[str, Any]) -> None:
        user = dependencies_dict["user"]
        assert_user_can_admin_create_company(user)
        dependencies_dict.setdefault("admin_create_user", user)

    def _inject_admin_user(self, dependencies_dict: dict[str, Any]) -> None:
        user = dependencies_dict["user"]
        assert_admin_user(user)
        dependencies_dict.setdefault("admin_user", user)

    def _inject_company_identifier(self, dependencies_dict: dict[str, Any]) -> None:
        session = dependencies_dict["session"]
        company_identifier = self.get_company_identifier_from_request()
        company = (
            session.query(Company).filter(Company.identifier == company_identifier).one_or_none()
        )
        if not company:
            raise UnauthorizedError("No company found with the given identifier")

        user = dependencies_dict["user"]
        assert_user_can_access_company(user, company, session)

        dependencies_dict.setdefault("company_identifier", company_identifier)
        dependencies_dict.setdefault("company", company)

    def route(self, path: str, read_only=True, **kwargs: Any) -> Callable[..., Any]:
        def decorator(func):
            sig = inspect.signature(func)
            dependencies = sig.parameters.keys()
            internal_dependencies = resolve_dependencies(dependencies)

            @functools.wraps(func)
            def wrapper(*args, **wrapper_kwargs):
                if (
                    "session" not in internal_dependencies
                    and "company_session" not in internal_dependencies
                ):
                    return func(*args, **wrapper_kwargs)

                current_request_path = (
                    f"{self.current_request.method} - {self.current_request.path}"
                )
                dependencies_dict = {}

                def inject_dependencies(session):
                    dependencies_dict["session"] = session
                    if "user" in internal_dependencies:
                        self._inject_user(dependencies_dict)
                    if "company_identifier" in internal_dependencies:
                        self._inject_company_identifier(dependencies_dict)
                    if "admin_create_user" in internal_dependencies:
                        self._inject_admin_create_user(dependencies_dict)
                    if "admin_user" in internal_dependencies:
                        self._inject_admin_user(dependencies_dict)

                def apply_dependencies():
                    for key in dependencies & dependencies_dict.keys():
                        wrapper_kwargs[key] = dependencies_dict[key]
                    return func(*args, **wrapper_kwargs)

                # Only company_session needed - minimal session context
                if "company_session" in dependencies and "session" not in dependencies:
                    with new_session(comment=current_request_path, read_only=read_only) as session:
                        inject_dependencies(session)
                        tenant_url = tenant_url_from_identifier(
                            company_identifier=dependencies_dict["company_identifier"],
                            session=session,
                        )

                    with new_company_session(
                        tenant_url, comment=current_request_path, read_only=read_only
                    ) as company_session:
                        dependencies_dict["company_session"] = company_session
                        return apply_dependencies()

                # Session needed (with or without company_session)
                with new_session(comment=current_request_path, read_only=read_only) as session:
                    inject_dependencies(session)

                    if "company_session" not in internal_dependencies:
                        return apply_dependencies()

                    # company_session is needed - keep it nested within session context
                    tenant_url = tenant_url_from_identifier(
                        company_identifier=dependencies_dict["company_identifier"],
                        session=session,
                    )
                    with new_company_session(
                        tenant_url, comment=current_request_path, read_only=read_only
                    ) as company_session:
                        dependencies_dict["company_session"] = company_session
                        return apply_dependencies()

            return super(SuperBlueprint, self).route(path, **kwargs)(wrapper)

        return decorator
