"""FastAPI dependency injection — replaces SuperBlueprint's parameter-name DI.

Two parallel chains (read-only and read-write) built from shared factory
functions.  Routes choose which chain to use based on whether they modify data:

    session: Session = Depends(get_db_session)        # read-only
    session: Session = Depends(get_db_session_rw)     # read-write

FastAPI caches dependencies per-callable per-request, so every dep in the same
chain shares a single global DB session — matching SuperBlueprint's behaviour.
"""

import uuid
from collections.abc import Generator

from fastapi import Depends, Header, Request
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from chalicelib.controllers.permission import Role
from chalicelib.controllers.tenant.session import new_company_session
from chalicelib.controllers.tenant.utils import tenant_url_from_identifier
from chalicelib.controllers.user import UserController
from chalicelib.new.config.infra.envars.control import ADMIN_EMAILS
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.session import new_session
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.permission import Permission
from chalicelib.schema.models.user import User
from chalicelib.tenant_utils import check_body, check_domain, check_header, check_uri_params
from exceptions import ForbiddenError, UnauthorizedError

# ---------------------------------------------------------------------------
# Helpers (ported from superblueprint.py)
# ---------------------------------------------------------------------------


def _assert_user_can_access_company(
    user: User,
    company: Company,
    session: Session,
    role: Role = Role.OPERATOR,
) -> None:
    count = session.execute(
        select(func.count())
        .select_from(Permission)
        .where(
            Permission.user_id == user.id,
            Permission.company_id == company.id,
            Permission.role == role.name,
        )
    ).scalar()
    if not count:
        raise UnauthorizedError("No company found")


def _extract_company_identifier(request: Request, json_body: dict) -> Identifier:
    """Try body → domain → header → path params (mirrors SuperBlueprint order)."""
    headers = request.headers
    path_params = request.path_params

    company_id_str = (
        check_body(json_body)
        or check_domain(json_body)  # NOTE: mutates json_body (pops company_identifier from domain)
        or check_header(headers)
        or check_uri_params(path_params)
    )
    if not company_id_str:
        raise UnauthorizedError("No company identifier provided")
    return Identifier(uuid.UUID(company_id_str))


# ---------------------------------------------------------------------------
# Shared dependency: request body (used by both RO and RW chains)
# ---------------------------------------------------------------------------


async def get_json_body(request: Request) -> dict:
    """Backward-compat replacement for ``bp.current_request.json_body``."""
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return {}
    try:
        return await request.json()
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Dependency factories — each returns a new callable so FastAPI treats them
# as distinct dependencies (important for per-chain caching).
# ---------------------------------------------------------------------------


def _make_db_session(read_only: bool):
    def dep() -> Generator[Session, None, None]:
        label = "fastapi_ro" if read_only else "fastapi_rw"
        with new_session(comment=label, read_only=read_only) as session:
            yield session

    return dep


def _make_current_user(session_dep):
    def dep(
        session: Session = Depends(session_dep),
        access_token: str = Header(alias="access_token"),
    ) -> User:
        return UserController.get_by_token(access_token, session=session)

    return dep


def _make_company_identifier(session_dep, user_dep):
    def dep(
        request: Request,
        session: Session = Depends(session_dep),
        user: User = Depends(user_dep),
        json_body: dict = Depends(get_json_body),
    ) -> Identifier:
        company_identifier = _extract_company_identifier(request, json_body)
        company = (
            session.query(Company).filter(Company.identifier == company_identifier).one_or_none()
        )
        if not company:
            raise UnauthorizedError("No company found with the given identifier")
        _assert_user_can_access_company(user, company, session)
        return company_identifier

    return dep


def _make_company(session_dep, company_id_dep):
    def dep(
        session: Session = Depends(session_dep),
        company_identifier: Identifier = Depends(company_id_dep),
    ) -> Company:
        company = (
            session.query(Company).filter(Company.identifier == company_identifier).one_or_none()
        )
        if not company:
            raise UnauthorizedError("No company found with the given identifier")
        return company

    return dep


def _make_company_session(session_dep, company_id_dep, read_only: bool):
    def dep(
        company_identifier: Identifier = Depends(company_id_dep),
        session: Session = Depends(session_dep),
    ) -> Generator[Session, None, None]:
        tenant_url = tenant_url_from_identifier(company_identifier, session)
        label = "fastapi_tenant_ro" if read_only else "fastapi_tenant_rw"
        with new_company_session(tenant_url, comment=label, read_only=read_only) as cs:
            yield cs

    return dep


def _make_user_identifier(user_dep):
    def dep(user: User = Depends(user_dep)) -> Identifier:
        return user.identifier

    return dep


def _make_admin_user(user_dep):
    def dep(user: User = Depends(user_dep)) -> User:
        if user.email not in ADMIN_EMAILS:
            raise ForbiddenError("Only admin users can perform this action")
        return user

    return dep


def _make_admin_create_user(user_dep):
    def dep(user: User = Depends(user_dep)) -> User:
        if user.email not in ADMIN_EMAILS:
            raise UnauthorizedError(
                "User does not have permission to create a company. "
                "Please contact support if you believe this is an error."
            )
        return user

    return dep


# ---------------------------------------------------------------------------
# Read-only chain (default for GET / read routes)
# ---------------------------------------------------------------------------

get_db_session = _make_db_session(read_only=True)
get_current_user = _make_current_user(get_db_session)
get_company_identifier = _make_company_identifier(get_db_session, get_current_user)
get_company = _make_company(get_db_session, get_company_identifier)
get_company_session = _make_company_session(
    get_db_session,
    get_company_identifier,
    read_only=True,
)
get_user_identifier = _make_user_identifier(get_current_user)
get_admin_user = _make_admin_user(get_current_user)
get_admin_create_user = _make_admin_create_user(get_current_user)

# ---------------------------------------------------------------------------
# Read-write chain (for POST/PUT/DELETE routes that modify data)
# ---------------------------------------------------------------------------

get_db_session_rw = _make_db_session(read_only=False)
get_current_user_rw = _make_current_user(get_db_session_rw)
get_company_identifier_rw = _make_company_identifier(get_db_session_rw, get_current_user_rw)
get_company_rw = _make_company(get_db_session_rw, get_company_identifier_rw)
get_company_session_rw = _make_company_session(
    get_db_session_rw,
    get_company_identifier_rw,
    read_only=False,
)
get_user_identifier_rw = _make_user_identifier(get_current_user_rw)
get_admin_user_rw = _make_admin_user(get_current_user_rw)
get_admin_create_user_rw = _make_admin_create_user(get_current_user_rw)
