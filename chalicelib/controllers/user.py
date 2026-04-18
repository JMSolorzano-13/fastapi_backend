import base64
import contextlib
import hashlib
import hmac
import random
import string
import uuid
from typing import Any
from uuid import uuid4

from botocore.exceptions import ClientError
from chalice import BadRequestError, ForbiddenError, NotFoundError, UnauthorizedError
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import cognito_client
from chalicelib.controllers import (
    disable_if_dev,
    ensure_list,
    is_super_user,
    remove_super_user,
    scale_to_super_user,
)
from chalicelib.controllers.cognito import decode_token
from chalicelib.controllers.common import CommonController
from chalicelib.controllers.permission import PermissionController
from chalicelib.controllers.workspace import WorkspaceController
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.envars.control import ADMIN_EMAILS
from chalicelib.new.odoo.infra.odoo_checker_source_name import OdooCheckerSourceName
from chalicelib.schema.models import Company, Permission, User
from chalicelib.schema.models import Workspace as WorkspaceORM


def get_secret_hash(username: str, client_id: str, client_secret: str) -> str:
    message = username + client_id
    dig = hmac.new(
        key=client_secret.encode("utf-8"), msg=message.encode("utf-8"), digestmod=hashlib.sha256
    ).digest()
    return base64.b64encode(dig).decode()


def _get_cognito_sub(cognito_user: dict[str, Any]) -> str:
    attributes = cognito_user["UserAttributes"]
    for attribute in attributes:
        if attribute["Name"] == "sub":
            return attribute["Value"]
    raise NotFoundError("No cognito sub")


def random_password() -> str:
    """Generate a random password.
    Must be at least 8 characters long
    Must include at least one uppercase letter (A-Z)
    Must include at least one lowercase letter (a-z)
    Must include at least one number
    Must include at least one non-alphanumeric character (! @ # $ % ^ & * ( ) _ + - = [ ] { } | ')


    Returns:
        str: [description]
    """

    ## length of password from the user
    length = 8
    chars_collection = {
        string.ascii_uppercase: 1,
        string.ascii_lowercase: 1,
        string.digits: 1,
        string.punctuation: 1,
    }
    all_chars: list[str] = []
    for collection in chars_collection:
        all_chars.extend(collection)

    password = []
    for collection, min_chars in chars_collection.items():
        password.extend([random.choice(collection) for _ in range(min_chars)])

    while len(password) < length:
        password.append(random.choice(all_chars))

    random.shuffle(password)

    return "".join(password)


class NeedCognitoChallenge(Exception):
    name: str
    session: str

    def __init__(self, name: str, session: str):
        self.name = name
        self.session = session


class UserController(CommonController):
    model = User

    @classmethod
    def link_to_db_if_needed(cls, token: str, session: Session) -> User:
        token_data = decode_token(token)
        cognito_sub = token_data["sub"]
        email = token_data["name"]
        with contextlib.suppress(NotFoundError):
            return cls.get_by_cognito_sub(cognito_sub, session=session)

        user = cls.get_user_by_email(email, session=session)
        if not user:
            raise NotFoundError(f"User with email {email} not found")
        # if user.cognito_sub and user.cognito_sub != "NOT_YET":
        #     # TODO sg loggear que hay una asignación de cognito_sub
        #     raise ForbiddenError(
        #         f"User {user.email} already has a cognito_sub assigned: {user.cognito_sub}"
        #     )
        user.cognito_sub = cognito_sub
        session.commit()

        # TODO sg licencia
        return user

    @classmethod
    def get_or_create_from_email(cls, email: str, session: Session) -> User:
        """Get a user by email or create a new one if it doesn't exist."""
        user: User = cls.get_user_by_email(email, session=session)

        context = scale_to_super_user()
        if not user:
            user = cls.create(
                {
                    "name": email,
                    "email": email,
                    "cognito_sub": uuid.uuid4().hex,  # TODO sg
                    "phone": "3313603245",  # TODO sg
                },
                session=session,
                context=context,
            )
            session.commit()
        context["user"] = user
        if not user.workspace:
            cls.create_default_workspace(user, session=session, context=context)

        return user

    @classmethod
    def confirm_forgot(cls, email: str, verification_code: str, new_password: str):
        try:
            return cognito_client().confirm_forgot_password(
                ClientId=envars.COGNITO_CLIENT_ID,
                Username=email,
                ConfirmationCode=verification_code,
                Password=new_password,
            )
        except ClientError as e:
            raise BadRequestError(e) from e

    @classmethod
    def forgot_login(cls, email: str):
        try:
            response = cognito_client().forgot_password(
                ClientId=envars.COGNITO_CLIENT_ID,
                Username=email,
            )
        except Exception as e:
            raise ForbiddenError(e) from e
        return response["CodeDeliveryDetails"]

    @classmethod
    def signup(
        cls,
        name,
        email,
        password,
        source_name: str = None,
        phone: str = None,
        *,
        session: Session,
    ):
        # Use exists
        if session.query(session.query(User).filter(User.email == email).exists()).scalar():
            raise ForbiddenError("User already exists")

        if not envars.mock.ODOO:
            OdooCheckerSourceName(
                url=envars.ODOO_URL,
                port=envars.ODOO_PORT,
                db=envars.ODOO_DB,
                user=envars.ODOO_USER,
                password=envars.ODOO_PASSWORD,
                need_odoo=envars.NOTIFY_ODOO,
            ).get_source_id_by_name(source_name)

        context = scale_to_super_user()
        cognito_sub = cls._create_cognito_user(email, password)
        user = super().create(
            {
                "name": name,
                "email": email,
                "cognito_sub": cognito_sub,
                "source_name": source_name,
                "phone": phone,
            },
            session=session,
            context=context,
        )
        remove_super_user(context)
        context["user"] = user
        session.commit()
        cls.create_default_workspace(user, session=session, context=context)
        return user

    @classmethod
    def create_default_workspace(cls, user: User, *, session: Session, context=None):
        return WorkspaceController.create(
            {
                "name": f"{user.name}'s Workspace",
            },
            session=session,
            context=context,
        )

    @classmethod
    def _change_password(cls, email, password):  # TODO implement
        try:
            cognito_client().admin_set_user_password(
                UserPoolId=envars.COGNITO_USER_POOL_ID,
                Username=email,
                Password=password,
                Permanent=True,
            )
        except Exception as e:
            raise ForbiddenError(e) from e

    @classmethod
    def _create_cognito_user(cls, email: str, password: str) -> str:
        try:
            secret_hash = None
            if envars.COGNITO_CLIENT_SECRET:
                secret_hash = get_secret_hash(
                    username=email,
                    client_id=envars.COGNITO_CLIENT_ID,
                    client_secret=envars.COGNITO_CLIENT_SECRET,
                )
            sign_up_kwargs = {
                "ClientId": envars.COGNITO_CLIENT_ID,
                "Username": email,
                "Password": password,
                "UserAttributes": [{"Name": "email", "Value": email}],
            }
            if secret_hash is not None:
                sign_up_kwargs["SecretHash"] = secret_hash
            user = cognito_client().sign_up(**sign_up_kwargs)
        except Exception as e:
            raise ForbiddenError(e) from e
        return user["UserSub"]

    @classmethod
    def get_by_id(cls, id: int, *, session: Session):
        try:
            user = session.query(User).filter(User.id == id).one()
        except NoResultFound as e:
            raise NotFoundError(f"User id '{id}' not found") from e
        return user

    @classmethod
    def get_by_identifier(cls, identifier: uuid4, *, session: Session):
        try:
            user = session.query(User).filter(User.identifier == identifier).one()
        except NoResultFound as e:
            raise NotFoundError(f"User identifier '{id}' not found") from e
        return user

    @classmethod
    def get_by_token(cls, token: str, *, session: Session):
        try:
            decoded = decode_token(id_token=token)
        except Exception as e:
            raise UnauthorizedError("Invalid token") from e
        cognito_sub = decoded["sub"]
        return cls.get_by_cognito_sub(cognito_sub, session=session)

    @classmethod
    def get_by_cognito_sub(cls, cognito_sub: str, session: Session) -> User:
        try:
            user: User = session.query(User).filter(User.cognito_sub == cognito_sub).one()
        except NoResultFound as e:
            raise NotFoundError(f"User '{cognito_sub}' not found") from e
        return user

    @classmethod
    def get_basic_info(cls, user: User, *, session: Session) -> dict[str, Any]:
        session.add(user)
        return {
            "id": user.id,
            "name": user.name,
            "email": user.email,
        }

    @classmethod
    def get_company_access(
        cls, user: User, company: Company, *, session: Session
    ) -> dict[str, Any]:
        session.add(user)
        session.add(company)
        return {
            "id": company.id,  # TODO remove
            "name": company.name,
            "modules": PermissionController.get_modules_available(user, company, session=session),
        }

    @classmethod
    def get_access(cls, user: User, *, session: Session):
        session.add(user)
        allowed_companies = CommonController.get_user_companies(user, session=session)
        allowed_workspaces = {company.workspace for company in allowed_companies} | set(
            CommonController.get_owned_by(user, session=session)
        )
        companies_by_ws = {
            workspace: {company for company in allowed_companies if company.workspace == workspace}
            for workspace in allowed_workspaces
        }
        companies_by_ws: dict[WorkspaceORM, set[Company]] = companies_by_ws
        return {
            workspace.identifier: {
                "id": workspace.id,  # TODO remove
                "license": workspace.license,
                "name": workspace.name,
                "stripe_status": workspace.stripe_status,
                "owner_id": workspace.owner_id,
                "pasto_worker_id": workspace.pasto_worker_id,
                "pasto_license_key": workspace.pasto_license_key,
                "pasto_installed": workspace.pasto_installed,
                "companies": {
                    company.identifier: cls.get_company_access(user, company, session=session)
                    for company in companies
                },
            }
            for workspace, companies in companies_by_ws.items()
        }

    @classmethod
    def get_info(cls, user: User, *, session: Session, context=None) -> dict[str, Any]:
        session.add(user)
        context_user = context["user"]
        session.add(context_user)
        if not is_super_user(context) and context_user != user:
            raise ForbiddenError(
                f"{cls.log_records(context_user)} not allowed "
                f"to see info of {cls.log_records(user)}"
            )
        return {
            "user": cls.get_basic_info(user, session=session),
            "access": cls.get_access(user, session=session),
        }

    @classmethod
    def get_user_by_email(cls, email: str, *, session: Session) -> User:
        """Get a user by email."""
        return session.query(User).filter(User.email == email).first()

    @classmethod
    def ensure_exist(cls, email: str, *, session: Session, context=None) -> User:
        """If the email exists, returns the user; else create a new one"""
        user = cls.get_user_by_email(email, session=session)
        if user is None:
            user = cls._invite(email, session=session, context=context)
            session.commit()
        return user

    @classmethod
    def _invite(cls, email: str, *, session: Session, context=None) -> User:
        """Invite a user to the system."""
        context_user = context.get("user")
        scale_to_super_user(context)
        session.add(context_user)
        cognito_sub = _admin_create_or_get_cognito_user(email)
        return cls.create(
            {
                "name": email,  # Al invitar, el nombre es el email
                "email": email,
                "invited_by_id": context_user and context_user.id or None,
                "cognito_sub": cognito_sub,
            },
            session=session,
            context=context,
        )

    @classmethod
    def auth_challenge(cls, challenge_name, challenge_session, email, password) -> dict[str, str]:
        if challenge_name != "NEW_PASSWORD_REQUIRED":
            raise UnauthorizedError(f"Challenge '{challenge_name}' not implemented")

        try:
            res = cognito_client().respond_to_auth_challenge(
                ClientId=envars.COGNITO_CLIENT_ID,
                ChallengeName=challenge_name,
                Session=challenge_session,
                ChallengeResponses={
                    "USERNAME": email,
                    "NEW_PASSWORD": password,  # TODO more options
                },
            )
        except Exception as e:
            raise ForbiddenError(e) from e
        else:
            return res["AuthenticationResult"]

    @classmethod
    def auth(cls, flow: str, params: dict[str, str]) -> dict[str, Any]:
        """Authenticate a user.

        Args:
            flow (str): Cognito AuthFlow
            params (Dict[str, str]): Cognito params

        Returns:
            Dict[str, Any]: AuthenticationResult from cognito_client().
        """
        if envars.COGNITO_CLIENT_SECRET:
            params["SECRET_HASH"] = get_secret_hash(
                username=params["USERNAME"],
                client_id=envars.COGNITO_CLIENT_ID,
                client_secret=envars.COGNITO_CLIENT_SECRET,
            )
        try:
            res = cognito_client().initiate_auth(
                ClientId=envars.COGNITO_CLIENT_ID,
                AuthFlow=flow,
                AuthParameters=params,
            )
        except ClientError as e:
            raise e
        if res.get("ChallengeName"):  # TODO Send extra parameters
            raise NeedCognitoChallenge(res["ChallengeName"], res["Session"])
        return res["AuthenticationResult"]

    @classmethod
    def change_password(cls, email: str, current_password: str, new_password: str, token: str):
        """Change the password of a user."""
        try:
            res = cognito_client().change_password(
                PreviousPassword=current_password,
                ProposedPassword=new_password,
                AccessToken=token,
            )
        except Exception as e:
            raise ForbiddenError(e) from e
        return res

    @staticmethod
    @disable_if_dev
    def ensure_external_super_user(user: User, message="do this", *, session: Session):
        """Check if the user is a super admin."""
        session.add(user)
        is_super = user.email in ADMIN_EMAILS

        if not is_super:
            raise ForbiddenError(f"Only external super users can {message}")

    @classmethod
    @ensure_list
    def check_companies(cls, records: list[User], *, session: Session, context=None):
        user = context["user"]
        for record in records:
            if record != user:
                raise ForbiddenError("Not allowed to modify other users")

    @classmethod
    def is_super_admin(cls, user: User) -> bool:
        """Check if the user is a super admin."""
        return user.email in ADMIN_EMAILS  # TODO use another technique

    @classmethod
    def find_existing_users(cls, emails: list[str], *, session: Session) -> dict[str, User]:
        if not emails:
            return {}

        users = session.query(User).filter(User.email.in_(emails)).all()
        return {user.email: user for user in users}

    @classmethod
    def identify_new_users_to_create(
        cls, emails: list[str], existing_users: dict[str, User]
    ) -> list[str]:
        return list(set(emails) - existing_users.keys())

    @classmethod
    def find_companies_by_identifiers(
        cls, company_identifiers: set[str], *, session: Session
    ) -> dict[str, Company]:
        companies = session.query(Company).filter(Company.identifier.in_(company_identifiers)).all()

        cls.validate_all_companies_found(company_identifiers, companies)

        return {company.identifier: company for company in companies}

    @classmethod
    def validate_all_companies_found(
        cls, requested_identifiers: set[str], found_companies: list[Company]
    ) -> None:
        if len(found_companies) != len(requested_identifiers):
            found_identifiers = {company.identifier for company in found_companies}
            missing = requested_identifiers - found_identifiers
            raise NotFoundError(f"Companies not found: {missing}")

    @classmethod
    def validate_license_limits(
        cls, new_users_count: int, companies: list[Company], *, session: Session
    ) -> None:
        if new_users_count == 0:
            return

        workspace_id = cls.get_workspace_from_companies(companies)
        workspace = cls._get_workspace_by_id(workspace_id, session=session)

        max_enrolls = cls._get_workspace_max_enrolls(workspace)
        if max_enrolls == "unlimited":
            return

        current_enrolled = cls._count_current_enrolled_users(workspace.id, session=session)

        needed_seats = current_enrolled + new_users_count
        if needed_seats > max_enrolls:
            raise ForbiddenError(
                f"License limit exceeded. Current: {current_enrolled}, "
                f"Attempting to add: {new_users_count}, "
                f"Limit: {max_enrolls}"
            )

    @classmethod
    def get_workspace_from_companies(cls, companies: list[Company]) -> int:
        workspace_ids = {company.workspace_id for company in companies}
        if len(workspace_ids) > 1:
            raise BadRequestError("Cannot invite to companies from multiple workspaces")

        return workspace_ids.pop()

    @classmethod
    def _get_workspace_by_id(cls, workspace_id: int, *, session: Session) -> WorkspaceORM:
        workspace = session.query(WorkspaceORM).filter(WorkspaceORM.id == workspace_id).first()
        if not workspace:
            raise NotFoundError(f"Workspace {workspace_id} not found")
        return workspace

    @classmethod
    def _get_workspace_max_enrolls(cls, workspace: WorkspaceORM) -> int | str:
        from chalicelib.controllers.workspace import WorkspaceController

        return WorkspaceController.license_attrib("max_emails_enroll", workspace)

    @classmethod
    def _count_current_enrolled_users(cls, workspace_id: int, *, session: Session) -> int:
        return (
            session.query(User.id)
            .join(Permission, Permission.user_id == User.id)
            .join(Company, Company.id == Permission.company_id)
            .filter(Company.workspace_id == workspace_id)
            .distinct()
            .count()
        )

    @classmethod
    def validate_inviter_owns_companies(
        cls, inviter: User, companies: list[Company], *, session: Session
    ) -> None:
        """Verificar que el invitador puede asignar permisos en todas las empresas.

        Responsabilidad única: Validación de permisos para asignar roles.
        Esta validación verifica que el usuario puede operar en el workspace de cada empresa.
        """
        if cls.is_super_admin(inviter):
            return

        # Validar para cada empresa que el usuario puede asignar permisos
        for company in companies:
            company_workspace = company.workspace

            if not WorkspaceController.user_is_owner_or_invited(inviter, company_workspace):
                raise ForbiddenError(
                    f"User {inviter.email} cannot assign permissions in company "
                    f"{company.identifier} from workspace {company_workspace.name}"
                )

    @classmethod
    def validate_permission_roles(cls, permissions_by_company: dict[str, set[str]]) -> None:
        """Validar que todos los roles especificados son válidos.

        Responsabilidad única: Validación de roles de permisos.
        """
        for company_identifier, roles in permissions_by_company.items():
            for role_str in roles:
                try:
                    Permission.RoleEnum[role_str.upper()]
                except KeyError as e:
                    raise BadRequestError(
                        f"Invalid role '{role_str}' for company {company_identifier}"
                    ) from e

    @classmethod
    def create_new_users(
        cls, new_user_emails: list[str], inviter: User, *, context: dict, session: Session
    ) -> dict[str, User]:
        new_users = {}
        for email in new_user_emails:
            user = cls._create_single_user(email, inviter, context=context, session=session)
            new_users[email] = user
        return new_users

    @classmethod
    def _create_single_user(
        cls, email: str, inviter: User, *, context: dict, session: Session
    ) -> User:
        scale_to_super_user(context)
        cognito_sub = _admin_create_or_get_cognito_user(email)
        user = cls.create(
            {
                "name": email,  # Al invitar, el nombre es el email
                "email": email,
                "invited_by_id": inviter.id,
                "cognito_sub": cognito_sub,
            },
            session=session,
            context=context,
        )
        return user

    @classmethod
    def delete_existing_permissions(
        cls, user_ids: list[int], company_ids: list[int], *, session: Session
    ) -> None:
        if not user_ids or not company_ids:
            return

        session.query(Permission).filter(
            Permission.user_id.in_(user_ids), Permission.company_id.in_(company_ids)
        ).delete(synchronize_session=False)

    @classmethod
    def create_permissions(
        cls,
        users_dict: dict[str, User],
        companies_dict: dict[str, Company],
        permissions_by_company: dict[str, set[str]],
        *,
        session: Session,
    ) -> None:
        permission_mappings = cls._build_permission_mappings(
            users_dict, companies_dict, permissions_by_company
        )

        if permission_mappings:
            session.bulk_insert_mappings(Permission, permission_mappings)

    @classmethod
    def _build_permission_mappings(
        cls,
        users_dict: dict[str, User],
        companies_dict: dict[str, Company],
        permissions_by_company: dict[str, set[str]],
    ) -> list[dict]:
        from uuid import uuid4

        # Pre-cargar datos una sola vez
        user_ids = list(users_dict.values())  # Obtener objetos User directamente
        company_ids_by_identifier = {
            identifier: company.id for identifier, company in companies_dict.items()
        }

        # Pre-convertir todos los roles únicos a enums
        all_unique_roles = {
            role_string.upper()
            for role_strings in permissions_by_company.values()
            for role_string in role_strings
        }
        role_enum_cache = {role: Permission.RoleEnum[role] for role in all_unique_roles}

        # Generar mappings usando list comprehension (más eficiente que loops)
        return [
            {
                "user_id": user.id,
                "company_id": company_ids_by_identifier[company_identifier],
                "role": role_enum_cache[role_string.upper()],
                "identifier": str(uuid4()),
            }
            for user in user_ids
            for company_identifier, role_strings in permissions_by_company.items()
            for role_string in role_strings
        ]

    @classmethod
    def manage_invited_by_id_after_permissions_created(
        cls, users_dict: dict[str, User], inviter: User, *, session: Session
    ) -> None:
        """
        Maneja invited_by_id DESPUÉS de crear permisos, basándose en el estado final:
        - Si un usuario es OWNER de un workspace -> NO tocar invited_by_id
        - Si un usuario tiene 0 permisos -> invited_by_id = NULL
        - Si un usuario tiene >= 1 permisos Y invited_by_id es NULL -> invited_by_id = inviter.id
        """
        if not users_dict:
            return

        # Importar WorkspaceORM para verificar owners
        from chalicelib.schema.models.workspace import Workspace as WorkspaceORM

        for user in users_dict.values():
            # Verificar si el usuario es owner de algún workspace
            is_workspace_owner = (
                session.query(WorkspaceORM).filter(WorkspaceORM.owner_id == user.id).first()
                is not None
            )

            # Si es owner de workspace, NO tocar invited_by_id
            if is_workspace_owner:
                continue

            # Para usuarios que NO son owners, aplicar lógica normal
            permission_count = (
                session.query(Permission).filter(Permission.user_id == user.id).count()
            )

            if permission_count == 0:
                # Usuario sin permisos -> limpiar invited_by_id
                if user.invited_by_id is not None:
                    user.invited_by_id = None
            else:
                # Usuario con permisos -> restaurar invited_by_id si es NULL
                if user.invited_by_id is None:
                    user.invited_by_id = inviter.id

    @classmethod
    @ensure_list
    def set_permissions(
        cls,
        emails: list[str],
        permissions_by_company: dict[str, set[str]],
        *,
        session: Session,
        context=None,
    ):
        invitador = context.get("user")
        session.add(invitador)

        # 1. Buscar usuarios existentes y identificar cuáles crear
        usuarios_existentes = cls.find_existing_users(emails, session=session)
        usuarios_nuevos_a_crear = cls.identify_new_users_to_create(emails, usuarios_existentes)

        # 2. Buscar y validar empresas
        company_identifiers = set(permissions_by_company.keys())
        companies_dict = cls.find_companies_by_identifiers(company_identifiers, session=session)
        companies = list(companies_dict.values())

        # 3. Validar límites de licencia
        cls.validate_license_limits(len(usuarios_nuevos_a_crear), companies, session=session)

        # 4. Validar permisos del invitador sobre las empresas
        cls.validate_inviter_owns_companies(invitador, companies, session=session)

        # 5. Validar roles especificados
        cls.validate_permission_roles(permissions_by_company)

        # (ZONA DE CONFIANZA)

        # 7. Crear usuarios faltantes
        nuevos_usuarios = cls.create_new_users(
            usuarios_nuevos_a_crear, invitador, context=context, session=session
        )

        # 8. Combinar todos los usuarios
        all_users = {**usuarios_existentes, **nuevos_usuarios}

        # 9. Eliminar permisos existentes
        user_ids = [user.id for user in all_users.values()]
        company_ids = [company.id for company in companies]
        cls.delete_existing_permissions(user_ids, company_ids, session=session)

        # 10. Crear nuevos permisos
        cls.create_permissions(all_users, companies_dict, permissions_by_company, session=session)

        # 11. Manejar invited_by_id basándose en el estado final de permisos
        cls.manage_invited_by_id_after_permissions_created(all_users, invitador, session=session)

        return list(all_users.values())


type CognitoSub = str


def _admin_create_or_get_cognito_user(email: str) -> CognitoSub:
    password = random_password()
    try:
        user = cognito_client().admin_create_user(
            UserPoolId=envars.COGNITO_USER_POOL_ID,
            Username=email,
            UserAttributes=[{"Name": "email", "Value": email}],
            TemporaryPassword=password,
            DesiredDeliveryMediums=[
                "EMAIL",
            ],
        )
    except cognito_client().exceptions.UsernameExistsException:
        user_existing = cognito_client().admin_get_user(
            UserPoolId=envars.COGNITO_USER_POOL_ID, Username=email
        )
        return next(
            attribute["Value"]
            for attribute in user_existing["UserAttributes"]
            if attribute["Name"] == "sub"
        )
    except Exception as e:
        raise ForbiddenError(e) from e
    attrs = user["User"]["Attributes"]
    return next(attribute["Value"] for attribute in attrs if attribute["Name"] == "sub")
