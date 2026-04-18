"""
Tests for company creation with authorization rules (FIS-157 fix).

Validates that CompanyController.create() correctly enforces:
- Owners can create companies in their workspace
- Invited users can create companies in the inviter's workspace
- Users with dual roles can operate in multiple workspaces based on context
- Unauthorized users cannot create companies in unrelated workspaces
"""

import json
import uuid
from unittest.mock import patch

import pytest
from chalice import ForbiddenError
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import cognito_client
from chalicelib.controllers import scale_to_super_user
from chalicelib.controllers.company import CompanyController
from chalicelib.new.config.infra import envars
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.permission import Permission
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace
from tests.fixtures.company import read_and_encode
from tests.fixtures.factories.company import CompanyFactory


def test_create_company():
    company = CompanyFactory.build(name="Test Company")

    assert company.name == "Test Company"


def test_create_user_with_company_integration(client, session):
    """
    Test completo que verifica:
    1. Registro de usuario
    2. Login del usuario
    3. Creación de company
    4. Verificación de permisos usando el nuevo flujo
    5. Verificación que los campos de email se llenan automáticamente
    """
    # Generar email único para evitar conflictos
    test_id = str(uuid.uuid4())[:8]
    test_email = f"integration-test-{test_id}@test.com"
    test_password = "IntegrationTest123!"

    # 1. Registrar usuario
    response_register = client.http.post(
        "/User",
        body=json.dumps(
            {
                "email": test_email,
                "name": "Test User Alonso",
                "phone": "1234567890",
                "password": test_password,
                "source_name": "facebook",
            }
        ),
        headers={"Content-Type": "application/json"},
    )

    assert response_register.status_code == 200, (
        f"Registration failed: {response_register.json_body}"
    )

    # Confirmar usuario en Cognito (necesario para tests)

    cognito_client().admin_confirm_sign_up(
        UserPoolId=envars.COGNITO_USER_POOL_ID,
        Username=test_email,
    )

    # 2. Login del usuario
    response_login = client.http.post(
        "/User/auth",
        body=json.dumps(
            {
                "flow": "USER_PASSWORD_AUTH",
                "params": {
                    "USERNAME": test_email,
                    "PASSWORD": test_password,
                },
            }
        ),
        headers={"Content-Type": "application/json"},
    )

    assert response_login.status_code == 200, f"Login failed: {response_login.json_body}"

    # 3. Verificar que se creó el usuario y workspace correctamente
    user = session.query(User).filter(User.email == test_email).one()
    workspace = session.query(Workspace).filter(Workspace.owner_id == user.id).one()

    assert user.email == test_email
    assert user.invited_by_id is None  # Es owner, no invitado
    assert workspace.owner_id == user.id

    token = response_login.json_body["AccessToken"]

    # 4. Crear una company usando certificados de test (usar company1 que siempre existe)

    cer_encoded = read_and_encode("tests/load_data/companies/company1/certificado.cer")
    key_encoded = read_and_encode("tests/load_data/companies/company1/llave.key")
    password = "12345678a"

    response_company = client.http.post(
        "/Company",
        body=json.dumps(
            {
                "cer": cer_encoded,
                "key": key_encoded,
                "pas": password,
                "workspace_id": workspace.id,
                "workspace_identifier": str(workspace.identifier),
            }
        ),
        headers={
            "Content-Type": "application/json",
            "access_token": token,
        },
    )

    assert response_company.status_code == 200, (
        f"Company creation failed: {response_company.json_body}"
    )

    # 5. Verificar que se creó la company correctamente
    company = (
        session.query(Company)
        .filter(
            Company.workspace_id == workspace.id,
        )
        .one()
    )

    assert company.workspace_id == workspace.id
    assert company.active is True

    # 6. Verificar que se crearon los permisos usando el nuevo flujo
    permissions = (
        session.query(Permission)
        .filter(
            Permission.user_id == user.id,
            Permission.company_id == company.id,
        )
        .all()
    )

    # Debe tener permisos OPERATOR y PAYROLL (creados por create_first_roles)
    assert len(permissions) == 2
    roles = {perm.role for perm in permissions}
    assert Permission.RoleEnum.OPERATOR in roles
    assert Permission.RoleEnum.PAYROLL in roles

    # 7. Verificar que invited_by_id no se modificó (debe seguir siendo None para owners)
    session.refresh(user)
    session.commit()
    assert user.invited_by_id is None

    # 8. Verify email population (FIS-500 Refactor)
    # 8. Verify email population (FIS-500 Refactor)
    expected_emails = [test_email]
    assert company.emails_to_send_efos == expected_emails
    assert company.emails_to_send_errors == expected_emails
    assert company.emails_to_send_canceled == expected_emails
    # assert que este probando que el correo del usuario forme parte de las listas de correos
    assert test_email in company.emails_to_send_efos
    assert test_email in company.emails_to_send_errors
    assert test_email in company.emails_to_send_canceled

    print("✅ Test completado exitosamente:")
    print(f"   - Usuario: {user.email}")
    print(f"   - Workspace: {workspace.name}")
    print(f"   - Company: {company.name}")
    print(f"   - Permisos: {len(permissions)}")
    print("   - Emails Populated: True")


# Fixtures para tests de PIB FIS-157


@pytest.fixture
def workspace_with_license(session: Session):
    """Workspace con licencia activa."""
    workspace = Workspace(
        name="Test Workspace",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspace)
    session.flush()
    return workspace


@pytest.fixture
def owner_user(session: Session, workspace_with_license):
    """Usuario owner con workspace asignado."""
    owner = User(
        identifier=str(uuid.uuid4()),
        name="Test Owner",
        email=f"owner_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="1234567890",
    )
    session.add(owner)
    session.flush()

    workspace_with_license.owner_id = owner.id
    session.flush()

    return owner


@pytest.fixture
def guest_user(session: Session, owner_user):
    """Usuario invitado por el owner."""
    guest = User(
        identifier=str(uuid.uuid4()),
        name="Test Guest",
        email=f"guest_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="0987654321",
        invited_by_id=owner_user.id,
    )
    session.add(guest)
    session.flush()
    return guest


# Tests de PIB FIS-157


def test_owner_can_create_company_in_own_workspace(
    session: Session, owner_user, workspace_with_license
):
    """Owner crea empresa exitosamente en su workspace."""
    context = scale_to_super_user()
    context["user"] = owner_user

    company_data = {
        "name": "Test Company Owner",
        "rfc": f"TST{uuid.uuid4().hex[:10].upper()}",
        "workspace_id": workspace_with_license.id,
        "workspace_identifier": str(workspace_with_license.identifier),
    }

    company = CompanyController.create(company_data, session=session, context=context)

    assert company.workspace_id == workspace_with_license.id
    assert company.rfc == company_data["rfc"]


def test_guest_can_create_company_in_owner_workspace(
    session: Session, guest_user, owner_user, workspace_with_license
):
    """Invitado crea empresa exitosamente en workspace del invitador."""
    context = scale_to_super_user()
    context["user"] = guest_user

    company_data = {
        "name": "Test Company Guest",
        "rfc": f"GST{uuid.uuid4().hex[:10].upper()}",
        "workspace_id": workspace_with_license.id,
        "workspace_identifier": str(workspace_with_license.identifier),
    }

    company = CompanyController.create(company_data, session=session, context=context)

    assert company.workspace_id == workspace_with_license.id
    assert company.rfc == company_data["rfc"]


def test_user_cannot_create_company_in_another_workspace(session: Session):
    """Usuario no autorizado no puede crear empresa en workspace no relacionado."""
    workspace1 = Workspace(
        name="Workspace 1",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspace1)
    session.flush()

    owner1 = User(
        identifier=str(uuid.uuid4()),
        name="Owner 1",
        email=f"owner1_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="1111111111",
    )
    session.add(owner1)
    session.flush()
    workspace1.owner_id = owner1.id
    session.flush()

    workspace2 = Workspace(
        name="Workspace 2",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspace2)
    session.flush()

    owner2 = User(
        identifier=str(uuid.uuid4()),
        name="Owner 2",
        email=f"owner2_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="2222222222",
    )
    session.add(owner2)
    session.flush()
    workspace2.owner_id = owner2.id
    session.flush()

    context = scale_to_super_user()
    context["user"] = owner2

    company_data = {
        "name": "Unauthorized Company",
        "rfc": f"UNA{uuid.uuid4().hex[:10].upper()}",
        "workspace_id": workspace1.id,
        "workspace_identifier": str(workspace1.identifier),
    }

    with pytest.raises(ForbiddenError, match="can not create company"):
        CompanyController.create(company_data, session=session, context=context)


def test_dual_role_user_can_create_in_owned_workspace(session: Session):
    """Usuario con roles duales (owner + invitado) puede crear en workspace propio."""
    workspaceA = Workspace(
        name="Workspace A (owned)",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspaceA)
    session.flush()

    workspaceB = Workspace(
        name="Workspace B",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspaceB)
    session.flush()

    ownerB = User(
        identifier=str(uuid.uuid4()),
        name="Owner B",
        email=f"ownerb_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="5555555555",
    )
    session.add(ownerB)
    session.flush()
    workspaceB.owner_id = ownerB.id
    session.flush()

    dual_role_user = User(
        identifier=str(uuid.uuid4()),
        name="Dual Role User",
        email=f"dual_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="6666666666",
        invited_by_id=ownerB.id,
    )
    session.add(dual_role_user)
    session.flush()
    workspaceA.owner_id = dual_role_user.id
    session.flush()

    context = scale_to_super_user()
    context["user"] = dual_role_user

    company_data = {
        "name": "Company in Own Workspace",
        "rfc": f"OWN{uuid.uuid4().hex[:10].upper()}",
        "workspace_id": workspaceA.id,
        "workspace_identifier": str(workspaceA.identifier),
    }

    company = CompanyController.create(company_data, session=session, context=context)

    assert company.workspace_id == workspaceA.id
    assert company.rfc == company_data["rfc"]


def test_dual_role_user_can_create_in_invited_workspace(session: Session):
    """Usuario con roles duales (owner + invitado) puede crear en workspace invitado."""
    workspaceA = Workspace(
        name="Workspace A (owned)",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspaceA)
    session.flush()

    workspaceB = Workspace(
        name="Workspace B (invited)",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspaceB)
    session.flush()

    ownerB = User(
        identifier=str(uuid.uuid4()),
        name="Owner B",
        email=f"ownerb_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="7777777777",
    )
    session.add(ownerB)
    session.flush()
    workspaceB.owner_id = ownerB.id
    session.flush()

    dual_role_user = User(
        identifier=str(uuid.uuid4()),
        name="Dual Role User",
        email=f"dual_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="8888888888",
        invited_by_id=ownerB.id,
    )
    session.add(dual_role_user)
    session.flush()
    workspaceA.owner_id = dual_role_user.id
    session.flush()

    context = scale_to_super_user()
    context["user"] = dual_role_user

    company_data = {
        "name": "Company in Invited Workspace",
        "rfc": f"INV{uuid.uuid4().hex[:10].upper()}",
        "workspace_id": workspaceB.id,
        "workspace_identifier": str(workspaceB.identifier),
    }

    company = CompanyController.create(company_data, session=session, context=context)

    assert company.workspace_id == workspaceB.id
    assert company.rfc == company_data["rfc"]


def test_dual_role_user_cannot_create_in_unrelated_workspace(session: Session):
    """Usuario con roles duales no puede crear en workspace no relacionado."""
    workspaceA = Workspace(
        name="Workspace A (owned)",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspaceA)
    session.flush()

    workspaceB = Workspace(
        name="Workspace B (invited)",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspaceB)
    session.flush()

    workspaceC = Workspace(
        name="Workspace C (unrelated)",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspaceC)
    session.flush()

    ownerB = User(
        identifier=str(uuid.uuid4()),
        name="Owner B",
        email=f"ownerb_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="9999999999",
    )
    session.add(ownerB)
    session.flush()
    workspaceB.owner_id = ownerB.id
    session.flush()

    ownerC = User(
        identifier=str(uuid.uuid4()),
        name="Owner C",
        email=f"ownerc_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="1010101010",
    )
    session.add(ownerC)
    session.flush()
    workspaceC.owner_id = ownerC.id
    session.flush()

    dual_role_user = User(
        identifier=str(uuid.uuid4()),
        name="Dual Role User",
        email=f"dual_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="1111111111",
        invited_by_id=ownerB.id,
    )
    session.add(dual_role_user)
    session.flush()
    workspaceA.owner_id = dual_role_user.id
    session.flush()

    context = scale_to_super_user()
    context["user"] = dual_role_user

    company_data = {
        "name": "Unauthorized Company",
        "rfc": f"UNR{uuid.uuid4().hex[:10].upper()}",
        "workspace_id": workspaceC.id,
        "workspace_identifier": str(workspaceC.identifier),
    }

    with pytest.raises(ForbiddenError, match="can not create company"):
        CompanyController.create(company_data, session=session, context=context)


# Tests de rollback de transacciones


def test_rollback_on_error_during_company_creation(session: Session):
    """
    Valida comportamiento de rollback cuando ocurre error después de flush pero antes de commit.

    Asegura que cambiar session.commit() a session.flush() permite que el context
    manager maneje el commit final, y cualquier error dispara rollback completo.
    """
    workspace = Workspace(
        name="Test Workspace Rollback",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspace)
    session.flush()

    owner = User(
        identifier=str(uuid.uuid4()),
        name="Test Owner",
        email=f"rollback_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="5555555555",
    )
    session.add(owner)
    session.flush()
    workspace.owner_id = owner.id
    session.flush()

    context = scale_to_super_user()
    context["user"] = owner

    company_data = {
        "name": "Test Company Rollback",
        "rfc": f"RBK{uuid.uuid4().hex[:10].upper()}",
        "workspace_id": workspace.id,
        "workspace_identifier": str(workspace.identifier),
    }

    companies_before = session.query(Company).filter(Company.workspace_id == workspace.id).count()

    with patch("chalicelib.controllers.company.UserController.set_permissions") as mock:
        mock.side_effect = Exception("Simulated error during role creation")

        with pytest.raises(Exception, match="Simulated error during role creation"):
            CompanyController.create(company_data, session=session, context=context)

    session.rollback()

    companies_after = session.query(Company).filter(Company.workspace_id == workspace.id).count()

    assert companies_after == companies_before
    assert (
        session.query(Company)
        .filter(Company.rfc == company_data["rfc"], Company.workspace_id == workspace.id)
        .first()
        is None
    )


def test_successful_company_creation_with_flush(session: Session):
    """
    Valida creación exitosa de empresa con flush en lugar de commit.

    Asegura que cuando no hay errores, la empresa se crea correctamente con
    asignación apropiada de ID y permisos.
    """
    workspace = Workspace(
        name="Test Workspace Success",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspace)
    session.flush()

    owner = User(
        identifier=str(uuid.uuid4()),
        name="Test Owner",
        email=f"success_{uuid.uuid4().hex[:8]}@test.com",
        cognito_sub=uuid.uuid4().hex,
        phone="6666666666",
    )
    session.add(owner)
    session.flush()
    workspace.owner_id = owner.id
    session.flush()

    context = scale_to_super_user()
    context["user"] = owner

    rfc = f"SUC{uuid.uuid4().hex[:10].upper()}"
    company_data = {
        "name": "Test Company Success",
        "rfc": rfc,
        "workspace_id": workspace.id,
        "workspace_identifier": str(workspace.identifier),
    }

    company = CompanyController.create(company_data, session=session, context=context)

    assert company.id is not None
    assert company.rfc == rfc
    assert company.workspace_id == workspace.id

    session.flush()

    company_check = session.query(Company).filter(Company.id == company.id).first()
    assert company_check is not None
    assert company_check.rfc == rfc

    permissions = (
        session.query(Permission)
        .filter(Permission.user_id == owner.id, Permission.company_id == company.id)
        .all()
    )

    assert len(permissions) == 2
    roles = {perm.role for perm in permissions}
    assert Permission.RoleEnum.OPERATOR in roles
    assert Permission.RoleEnum.PAYROLL in roles
