from typing import Any

from chalice import NotFoundError, Response
from chalice.app import BadRequestError
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
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

bp = SuperBlueprint(__name__)


@bp.route("/", methods=["PUT"], cors=common.cors_config, read_only=False)
def update(session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    values: dict[str, Any] = json_body.get("values", {})

    context = {"user": user}
    UserController.update(user, values, context=context, session=session)
    return UserController.to_nested_dict(user)


@bp.route("/", methods=["GET"], cors=common.cors_config)
def get(session: Session, user: User):
    context = {"user": user}
    return UserController.get_info(user, context=context, session=session)


@bp.route("/", methods=["POST"], cors=common.cors_config, read_only=False)
def create(session: Session):
    json_body = bp.current_request.json_body or {}
    name = json_body["name"]
    email = json_body["email"]
    password = json_body["password"]
    source_name = json_body.get("source_name", None)
    phone = json_body.get("phone", None)
    user = UserController.signup(name, email, password, source_name, phone, session=session)
    return UserController.to_nested_dict(user)


@bp.route("/change_password", methods=["POST"], cors=common.cors_config, read_only=False)
def change_password(session: Session):
    token = bp.current_request.headers["access_token"]
    json_body = bp.current_request.json_body or {}

    email = json_body["email"]
    current_password = json_body["current_password"]
    new_password = json_body["new_password"]

    return UserController.change_password(
        email, current_password, new_password, token, session=session
    )


@bp.route("/forgot", methods=["POST"], cors=common.cors_config)
def forgot():
    if envars.AUTH_BACKEND == "local_jwt":
        return Response(
            status_code=501,
            body={
                "Code": "NotSupportedForAuthModeError",
                "Message": "forgot_password is not available when AUTH_BACKEND=local_jwt",
            },
        )
    json_body = bp.current_request.json_body or {}
    email = json_body["email"]
    return UserController.forgot_login(email)


@bp.route("/confirm_forgot", methods=["POST"], cors=common.cors_config)
def confirm_forgot():
    if envars.AUTH_BACKEND == "local_jwt":
        return Response(
            status_code=501,
            body={
                "Code": "NotSupportedForAuthModeError",
                "Message": "confirm_forgot is not available when AUTH_BACKEND=local_jwt",
            },
        )
    json_body = bp.current_request.json_body or {}
    email = json_body["email"]
    verification_code = json_body["verification_code"]
    new_password = json_body["new_password"]
    return UserController.confirm_forgot(email, verification_code, new_password)


@bp.route("/config", methods=["POST"], cors=common.cors_config, read_only=False)
def post_config(company_session: Session, user_identifier: Identifier):
    json_body = bp.current_request.json_body or {}
    config = json_body["config"]

    config = UserConfigController.set_config(
        user_identifier=user_identifier,
        config=config,
        company_session=company_session,
    )
    return config.data


@bp.route("/config/{company_identifier}", methods=["GET"], cors=common.cors_config)
def get_config(company_identifier, company_session: Session, user_identifier: Identifier):
    config = UserConfigController.get_config(user_identifier, company_session=company_session)
    return config.data


@bp.route("/super_invite", methods=["POST"], cors=common.cors_config, read_only=False)
def super_invite(session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    email = json_body["email"]

    context = {"user": user}
    user_by_email = UserController.ensure_exist(email, context=context, session=session)
    return UserController.to_nested_dict(user_by_email)[0]


@bp.route("/auth", methods=["POST"], cors=common.cors_config)
def auth(session: Session):
    json_body = bp.current_request.json_body or {}

    flow = json_body["flow"]
    params = json_body["params"]

    if envars.BLOCK_APP_ACCESS:
        return Response(status_code=403, body={"state": f"{envars.BLOCK_APP_MESSAGE}"})

    try:
        return UserController.auth(flow, params, session=session)
    except NeedCognitoChallenge as e:
        return Response(
            status_code=428,
            body={  # TODO http status redirect
                "state": "need_cognito_challenge",
                "challenge_name": e.name,
                "challenge_session": e.session,
            },
        )


@bp.route("/auth/{code}", methods=["GET"], cors=common.cors_config, read_only=False)
def auth_by_code(code: str, session: Session):
    if envars.BLOCK_APP_ACCESS:
        return Response(status_code=403, body={"state": f"{envars.BLOCK_APP_MESSAGE}"})

    if envars.AUTH_BACKEND == "local_jwt":
        return Response(
            status_code=501,
            body={
                "Code": "NotSupportedForAuthModeError",
                "Message": "OAuth callback is not available when AUTH_BACKEND=local_jwt",
            },
        )

    tokens = cognito.exchange_code_for_tokens(code)
    id_token = tokens.get("id_token")
    UserController.link_to_db_if_needed(token=id_token, session=session)  # noqa: F841
    return tokens


@bp.route("/auth_challenge", methods=["POST"], cors=common.cors_config)
def auth_challenge():
    """Starts the flow to respond to an authorization challenge

    Args:
        challenge_name (str): The challenge name e.g. 'NEW_PASSWORD_REQUIRED'
        challenge_session (str): The challenge session
        email (str): The email of the user.
        password (str): The password of the user.
    """
    if envars.AUTH_BACKEND == "local_jwt":
        return Response(
            status_code=501,
            body={
                "Code": "NotSupportedForAuthModeError",
                "Message": "auth_challenge is not available when AUTH_BACKEND=local_jwt",
            },
        )

    json_body = bp.current_request.json_body or {}

    challenge_name = json_body["challenge_name"]
    challenge_session = json_body["challenge_session"]
    email = json_body["email"]
    password = json_body["password"]

    return UserController.auth_challenge(challenge_name, challenge_session, email, password)


@bp.route("/update_fiscal_data", methods=["POST"], cors=common.cors_config)
def update_fiscal_data(session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    try:
        data = FiscalData(**json_body)
    except TypeError as e:
        raise BadRequestError(str(e)) from e

    connection = OdooConnection()
    updater = FiscalDataUpdater(connection)
    try:
        updater.update(user.odoo_identifier, data)
    except FiscalDataUpdateError as e:
        raise BadRequestError(str(e)) from e


@bp.route("/update_fiscal_data", methods=["GET"], cors=common.cors_config)
def get_fiscal_data(session: Session, user: User):
    connection = OdooConnection()
    updater = FiscalDataUpdater(connection)
    try:
        data = updater.retrieve(user.odoo_identifier)
        return data.to_dict()
    except FiscalDataUpdateError as e:
        raise BadRequestError(str(e)) from e


@bp.route(
    "/set_email/{old_email}/{new_email}",
    methods=["PUT"],
    cors=common.cors_config,
    read_only=False,
)
def set_email(old_email: str, new_email: str, session: Session, admin_user: User):
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
    return Response(body="", status_code=200)
