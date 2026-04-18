import json
import uuid
from datetime import datetime

import pytest
from chalice.test import Client
from pydantic import BaseModel
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import cognito_client
from chalicelib.controllers.user import UserController
from chalicelib.new.config.infra import envars
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.permission import Permission
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace
from tests.fixtures.aws.cognito import ClientWithHeaders


class UserCredentials(BaseModel):
    email: str
    password: str


@pytest.fixture
def user_credentials() -> UserCredentials:
    return UserCredentials(
        email="user@test.com",
        password="Test123.",
    )


@pytest.fixture
def user_other_credentials() -> UserCredentials:
    return UserCredentials(
        email="user2@test.com",
        password="Test123.",
    )


@pytest.fixture
def user(
    client: Client,
    user_credentials: UserCredentials,
    session: Session,
) -> User:
    if _user := session.query(User).filter(User.email == user_credentials.email).first():
        _user.cognito_sub = UserController._create_cognito_user(
            email=user_credentials.email,
            password=user_credentials.password,
        )
        cognito_client().admin_confirm_sign_up(
            UserPoolId=envars.COGNITO_USER_POOL_ID,
            Username=user_credentials.email,
        )
        return _user

    response_register = client.http.post(
        "/User",
        body=json.dumps(
            {
                "email": user_credentials.email,
                "name": "Test User",
                "phone": "1234567890",
                "password": user_credentials.password,
                "source_name": "facebook",
            }
        ),
        headers={"Content-Type": "application/json"},
    )

    cognito_client().admin_confirm_sign_up(
        UserPoolId=envars.COGNITO_USER_POOL_ID,
        Username=user_credentials.email,
    )

    assert response_register.status_code == 200

    user = session.query(User).filter(User.email == user_credentials.email).one()

    return user


@pytest.fixture
def user_token(
    client: Client,
    user: User,  # Para que esté creado
    user_credentials: UserCredentials,
):
    response_login = client.http.post(
        "/User/auth",
        body=json.dumps(
            {
                "flow": "USER_PASSWORD_AUTH",
                "params": {
                    "USERNAME": user_credentials.email,
                    "PASSWORD": user_credentials.password,
                },
            }
        ),
        headers={"Content-Type": "application/json"},
    )

    assert response_login.status_code == 200

    token = response_login.json_body["AccessToken"]

    return token


@pytest.fixture
def workspace(user: User) -> Workspace:
    user.workspace.license = {
        "id": 1,
        "date_start": "2022-12-07",
        "date_end": "2026-03-08",
        "details": {
            "max_emails_enroll": "unlimited",
            "max_companies": "unlimited",
            "exceed_metadata_limit": True,
            "products": [
                {"identifier": "prod_MZAUw4gnheSoOT", "quantity": 1.0},
                {"identifier": "prod_MZAXIkc8Ns2BLb", "quantity": 3.0},
            ],
        },
        "stripe_status": "active",
    }
    return user.workspace


@pytest.fixture
def user_other(
    client: Client,
    session: Session,
    user_other_credentials: UserCredentials,
) -> User:
    response_register = client.http.post(
        "/User",
        body=json.dumps(
            {
                "email": user_other_credentials.email,
                "name": "Test User",
                "phone": "1234567890",
                "password": user_other_credentials.password,
                "source_name": "facebook",
            }
        ),
        headers={"Content-Type": "application/json"},
    )

    # Confirma el usuario en cognito
    cognito_client().admin_confirm_sign_up(
        UserPoolId=envars.COGNITO_USER_POOL_ID,
        Username=user_other_credentials.email,
    )

    assert response_register.status_code == 200

    user = session.query(User).filter(User.email == user_other_credentials.email).one()
    return user


@pytest.fixture
def user_other_token(
    client: Client,
    user_other: User,  # Para que esté creado
    user_other_credentials: UserCredentials,
):
    response_login = client.http.post(
        "/User/auth",
        body=json.dumps(
            {
                "flow": "USER_PASSWORD_AUTH",
                "params": {
                    "USERNAME": user_other_credentials.email,
                    "PASSWORD": user_other_credentials.password,
                },
            }
        ),
        headers={"Content-Type": "application/json"},
    )

    assert response_login.status_code == 200

    token = response_login.json_body["AccessToken"]

    return token


@pytest.fixture
def owner_user_with_companies(
    client: Client,
    session: Session,
):
    """
    Fixture que crea un usuario owner con workspace válido y 2 empresas de ejemplo.
    Incluye permisos de admin sobre las empresas.
    Útil para tests de asignación de permisos.
    """
    # Generar datos únicos para evitar conflictos
    test_id = str(uuid.uuid4())[:8]
    email = f"owner-{test_id}@test.com"
    password = "OwnerPass123!"

    # 1. Registrar usuario
    response_register = client.http.post(
        "/User",
        body=json.dumps(
            {
                "email": email,
                "name": f"Owner User {test_id}",
                "phone": "1234567890",
                "password": password,
                "source_name": "facebook",
            }
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response_register.status_code == 200

    # 2. Confirmar usuario en Cognito
    cognito_client().admin_confirm_sign_up(
        UserPoolId=envars.COGNITO_USER_POOL_ID,
        Username=email,
    )

    # 3. Login para obtener token
    response_login = client.http.post(
        "/User/auth",
        body=json.dumps(
            {
                "flow": "USER_PASSWORD_AUTH",
                "params": {
                    "USERNAME": email,
                    "PASSWORD": password,
                },
            }
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response_login.status_code == 200

    # 4. Obtener objetos de base de datos
    user = session.query(User).filter(User.email == email).one()
    workspace = session.query(Workspace).filter(Workspace.owner_id == user.id).one()

    # 5. Agregar licencia válida al workspace
    workspace.license = {
        "id": 1,
        "date_start": "2024-07-03",
        "date_end": "2027-07-03",
        "details": {
            "max_emails_enroll": 100,
            "max_companies": 50,
            "exceed_metadata_limit": False,
            "add_enabled": True,
            "products": [{"identifier": "prod_test", "quantity": 1}],
        },
        "stripe_status": "active",
    }
    session.add(workspace)
    session.flush()

    # 6. Crear empresas de ejemplo
    company1 = Company(
        identifier=str(uuid.uuid4()),
        name=f"Company 1 {test_id}",
        workspace_id=workspace.id,
        workspace_identifier=workspace.identifier,
        created_at=datetime.utcnow(),
    )
    company2 = Company(
        identifier=str(uuid.uuid4()),
        name=f"Company 2 {test_id}",
        workspace_id=workspace.id,
        workspace_identifier=workspace.identifier,
        created_at=datetime.utcnow(),
    )
    session.add(company1)
    session.add(company2)
    session.flush()

    # 7. Dar permisos de admin sobre las empresas
    perm_op_company1 = Permission(
        user_id=user.id,
        company_id=company1.id,
        role=Permission.RoleEnum.OPERATOR,
        identifier=str(uuid.uuid4()),
        created_at=datetime.utcnow(),
    )
    perm_pa_company1 = Permission(
        user_id=user.id,
        company_id=company1.id,
        role=Permission.RoleEnum.PAYROLL,
        identifier=str(uuid.uuid4()),
        created_at=datetime.utcnow(),
    )
    perm_op_company2 = Permission(
        user_id=user.id,
        company_id=company2.id,
        role=Permission.RoleEnum.OPERATOR,
        identifier=str(uuid.uuid4()),
        created_at=datetime.utcnow(),
    )
    perm_pa_company2 = Permission(
        user_id=user.id,
        company_id=company2.id,
        role=Permission.RoleEnum.PAYROLL,
        identifier=str(uuid.uuid4()),
        created_at=datetime.utcnow(),
    )
    session.add_all([perm_op_company1, perm_pa_company1, perm_op_company2, perm_pa_company2])
    session.commit()

    return {
        "token": response_login.json_body["AccessToken"],
        "user": user,
        "workspace": workspace,
        "companies": [company1, company2],
        "email": email,
        "test_id": test_id,
    }


@pytest.fixture
def other_owner_user_with_companies(
    client: Client,
    session: Session,
):
    """
    Fixture que crea un segundo usuario owner independiente con su workspace y empresas.
    Útil para tests que requieren validar conflictos entre workspaces diferentes.
    """
    # Generar datos únicos para evitar conflictos
    test_id = str(uuid.uuid4())[:8]
    email = f"other-owner-{test_id}@test.com"
    password = "OtherOwnerPass123!"

    # 1. Registrar usuario
    response_register = client.http.post(
        "/User",
        body=json.dumps(
            {
                "email": email,
                "name": f"Other Owner User {test_id}",
                "phone": "1234567890",
                "password": password,
                "source_name": "facebook",
            }
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response_register.status_code == 200

    # 2. Confirmar usuario en Cognito
    cognito_client().admin_confirm_sign_up(
        UserPoolId=envars.COGNITO_USER_POOL_ID,
        Username=email,
    )

    # 3. Login para obtener token
    response_login = client.http.post(
        "/User/auth",
        body=json.dumps(
            {
                "flow": "USER_PASSWORD_AUTH",
                "params": {
                    "USERNAME": email,
                    "PASSWORD": password,
                },
            }
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response_login.status_code == 200

    # 4. Obtener objetos de base de datos
    user = session.query(User).filter(User.email == email).one()
    workspace = session.query(Workspace).filter(Workspace.owner_id == user.id).one()

    # 5. Agregar licencia válida al workspace
    workspace.license = {
        "id": 1,
        "date_start": "2024-07-03",
        "date_end": "2027-07-03",
        "details": {
            "max_emails_enroll": 2,
            "max_companies": 50,
            "exceed_metadata_limit": False,
            "add_enabled": True,
            "products": [{"identifier": "prod_test", "quantity": 1}],
        },
        "stripe_status": "active",
    }
    session.add(workspace)
    session.flush()

    # 6. Crear empresas de ejemplo
    company1 = Company(
        identifier=str(uuid.uuid4()),
        name=f"Other Company 1 {test_id}",
        workspace_id=workspace.id,
        workspace_identifier=workspace.identifier,
        created_at=datetime.utcnow(),
    )
    company2 = Company(
        identifier=str(uuid.uuid4()),
        name=f"Other Company 2 {test_id}",
        workspace_id=workspace.id,
        workspace_identifier=workspace.identifier,
        created_at=datetime.utcnow(),
    )
    session.add(company1)
    session.add(company2)
    session.flush()

    # 7. Dar ambos permisos (OPERATOR y PAYROLL) al admin sobre las empresas
    permissions = [
        Permission(
            user_id=user.id,
            company_id=company1.id,
            role=Permission.RoleEnum.OPERATOR,
            identifier=str(uuid.uuid4()),
            created_at=datetime.utcnow(),
        ),
        Permission(
            user_id=user.id,
            company_id=company1.id,
            role=Permission.RoleEnum.PAYROLL,
            identifier=str(uuid.uuid4()),
            created_at=datetime.utcnow(),
        ),
        Permission(
            user_id=user.id,
            company_id=company2.id,
            role=Permission.RoleEnum.OPERATOR,
            identifier=str(uuid.uuid4()),
            created_at=datetime.utcnow(),
        ),
        Permission(
            user_id=user.id,
            company_id=company2.id,
            role=Permission.RoleEnum.PAYROLL,
            identifier=str(uuid.uuid4()),
            created_at=datetime.utcnow(),
        ),
    ]
    for perm in permissions:
        session.add(perm)
    session.commit()

    return {
        "token": response_login.json_body["AccessToken"],
        "user": user,
        "workspace": workspace,
        "companies": [company1, company2],
        "email": email,
        "test_id": test_id,
    }


@pytest.fixture
def client_authenticated(client: ClientWithHeaders, user_token: str) -> Client:
    client.http.default_headers.update(
        {
            "Content-Type": "application/json",
            "access_token": user_token,
        }
    )
    return client
