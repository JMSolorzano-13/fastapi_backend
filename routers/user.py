"""User routes — auth, CRUD, password management, config, fiscal data.

Ported from: backend/chalicelib/blueprints/user.py
15 routes total.
"""

from fastapi import APIRouter, Body, Depends, Header
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from chalicelib.controllers import cognito
from chalicelib.controllers.user import NeedCognitoChallenge, UserController
from chalicelib.controllers.user_config import UserConfigController
from chalicelib.new.config.infra import envars
from chalicelib.new.fiscal_data import (
    FiscalData,
    FiscalDataUpdateError,
    FiscalDataUpdater,
)
from chalicelib.new.odoo import OdooConnection
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.validators import is_valid_email
from chalicelib.schema.models.user import User
from dependencies import (
    get_admin_user_rw,
    get_company_session,
    get_company_session_rw,
    get_current_user,
    get_current_user_rw,
    get_db_session,
    get_db_session_rw,
    get_user_identifier,
    get_user_identifier_rw,
)
from exceptions import BadRequestError, NotFoundError, NotSupportedForAuthModeError

router = APIRouter(tags=["User"])


# ---------------------------------------------------------------------------
# Auth routes (no session/user injection)
# ---------------------------------------------------------------------------


@router.post("/auth")
def auth(body: dict = Body(...), session: Session = Depends(get_db_session)):
    flow = body["flow"]
    params = body["params"]

    if envars.BLOCK_APP_ACCESS:
        return JSONResponse(
            status_code=403,
            content={"state": f"{envars.BLOCK_APP_MESSAGE}"},
        )

    try:
        return UserController.auth(flow, params, session=session)
    except NeedCognitoChallenge as e:
        return JSONResponse(
            status_code=428,
            content={
                "state": "need_cognito_challenge",
                "challenge_name": e.name,
                "challenge_session": e.session,
            },
        )


@router.get("/auth/{code}")
def auth_by_code(code: str, session: Session = Depends(get_db_session_rw)):
    if envars.BLOCK_APP_ACCESS:
        return JSONResponse(
            status_code=403,
            content={"state": f"{envars.BLOCK_APP_MESSAGE}"},
        )

    if envars.AUTH_BACKEND == "local_jwt":
        raise NotSupportedForAuthModeError(
            "OAuth callback /auth/{code} is not available when AUTH_BACKEND=local_jwt"
        )

    tokens = cognito.exchange_code_for_tokens(code)
    id_token = tokens.get("id_token")
    UserController.link_to_db_if_needed(token=id_token, session=session)
    return tokens


@router.post("/auth_challenge")
def auth_challenge(body: dict = Body(...)):
    challenge_name = body["challenge_name"]
    challenge_session = body["challenge_session"]
    email = body["email"]
    password = body["password"]
    return UserController.auth_challenge(challenge_name, challenge_session, email, password)


# ---------------------------------------------------------------------------
# User CRUD
# ---------------------------------------------------------------------------


@router.post("", include_in_schema=False)
@router.post("/")
def create(body: dict = Body(...), session: Session = Depends(get_db_session_rw)):
    name = body["name"]
    email = body["email"]
    password = body["password"]
    source_name = body.get("source_name")
    phone = body.get("phone")
    user = UserController.signup(name, email, password, source_name, phone, session=session)
    return UserController.to_nested_dict(user)


@router.get("", include_in_schema=False)
@router.get("/")
def get(session: Session = Depends(get_db_session), user: User = Depends(get_current_user)):
    context = {"user": user}
    return UserController.get_info(user, context=context, session=session)


@router.put("", include_in_schema=False)
@router.put("/")
def update(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    values = body.get("values", {})
    context = {"user": user}
    UserController.update(user, values, context=context, session=session)
    return UserController.to_nested_dict(user)


# ---------------------------------------------------------------------------
# Password management (no DB session needed — calls Cognito directly)
# ---------------------------------------------------------------------------


@router.post("/change_password")
def change_password(
    body: dict = Body(...),
    access_token: str = Header(alias="access_token"),
    session: Session = Depends(get_db_session_rw),
):
    email = body["email"]
    current_password = body["current_password"]
    new_password = body["new_password"]
    return UserController.change_password(
        email, current_password, new_password, access_token, session=session
    )


@router.post("/forgot")
def forgot(body: dict = Body(...)):
    email = body["email"]
    return UserController.forgot_login(email)


@router.post("/confirm_forgot")
def confirm_forgot(body: dict = Body(...)):
    email = body["email"]
    verification_code = body["verification_code"]
    new_password = body["new_password"]
    return UserController.confirm_forgot(email, verification_code, new_password)


# ---------------------------------------------------------------------------
# User config (per-company, uses company_session)
# ---------------------------------------------------------------------------


@router.post("/config")
def post_config(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
    user_identifier: Identifier = Depends(get_user_identifier_rw),
):
    config = body["config"]
    config_obj = UserConfigController.set_config(
        user_identifier=user_identifier,
        config=config,
        company_session=company_session,
    )
    return config_obj.data


@router.get("/config/{company_identifier}")
def get_config(
    company_identifier: str,
    company_session: Session = Depends(get_company_session),
    user_identifier: Identifier = Depends(get_user_identifier),
):
    config = UserConfigController.get_config(user_identifier, company_session=company_session)
    return config.data if config else {}


# ---------------------------------------------------------------------------
# Admin / invite
# ---------------------------------------------------------------------------


@router.post("/super_invite")
def super_invite(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    email = body["email"]
    context = {"user": user}
    user_by_email = UserController.ensure_exist(email, context=context, session=session)
    return UserController.to_nested_dict(user_by_email)[0]


@router.put("/set_email/{old_email}/{new_email}")
def set_email(
    old_email: str,
    new_email: str,
    session: Session = Depends(get_db_session_rw),
    admin_user: User = Depends(get_admin_user_rw),
):
    new_email = new_email.strip().lower()
    old_email = old_email.strip().lower()

    if not is_valid_email(new_email):
        raise BadRequestError("Invalid email format")

    existing = session.query(User).filter(User.email == new_email).first()
    if existing:
        raise BadRequestError("Email is already in use")

    target_user = session.query(User).filter(User.email == old_email).first()
    if not target_user:
        raise NotFoundError(f"User with email '{old_email}' not found")

    target_user.email = new_email
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Fiscal data (Odoo integration)
# ---------------------------------------------------------------------------


@router.post("/update_fiscal_data")
def update_fiscal_data(
    body: dict = Body(...),
    session: Session = Depends(get_db_session),
    user: User = Depends(get_current_user),
):
    try:
        data = FiscalData(**body)
    except TypeError as e:
        raise BadRequestError(str(e)) from e

    connection = OdooConnection()
    updater = FiscalDataUpdater(connection)
    try:
        updater.update(user.odoo_identifier, data)
    except FiscalDataUpdateError as e:
        raise BadRequestError(str(e)) from e


@router.get("/update_fiscal_data")
def get_fiscal_data(
    session: Session = Depends(get_db_session),
    user: User = Depends(get_current_user),
):
    connection = OdooConnection()
    updater = FiscalDataUpdater(connection)
    try:
        data = updater.retrieve(user.odoo_identifier)
        return data.to_dict()
    except FiscalDataUpdateError as e:
        raise BadRequestError(str(e)) from e
