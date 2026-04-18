import json
from contextlib import contextmanager

import pytest
from chalice.test import Client

from chalicelib.schema.models import Permission, User, Workspace


@pytest.fixture(autouse=False)
def force_session_mock(session, mocker):
    """Forzar que el endpoint use la misma sesión que el test"""

    @contextmanager
    def mock_new_session(*args, **kwargs):
        yield session

    mocker.patch("chalicelib.new.utils.session.new_session", mock_new_session)
    mocker.patch("chalicelib.blueprints.superblueprint.new_session", mock_new_session)


def test_assign_permission_simple_flow(
    client: Client, session, owner_user_with_companies, force_session_mock
):
    owner_data = owner_user_with_companies
    admin_user = owner_data["user"]
    companies = owner_data["companies"]
    admin_token = owner_data["token"]

    initial_permissions = (
        session.query(Permission).filter(Permission.user_id == admin_user.id).all()
    )
    assert len(initial_permissions) == 4  # 2 roles × 2 empresas

    request_body = {
        "emails": [admin_user.email],
        "permissions": {
            str(companies[0].identifier): ["OPERATOR"],  # Solo OPERATOR (quita PAYROLL)
            str(companies[1].identifier): ["PAYROLL"],  # Solo PAYROLL (quita OPERATOR)
        },
    }

    response = client.http.put(
        "/Permission",
        body=json.dumps(request_body),
        headers={"Content-Type": "application/json", "access_token": admin_token},
    )

    assert response.status_code == 200
    response_data = response.json_body
    assert response_data["users_processed"] == 1
    assert response_data["companies_processed"] == 2

    final_permissions = session.query(Permission).filter(Permission.user_id == admin_user.id).all()

    assert len(final_permissions) == 2

    company1_permissions = [p for p in final_permissions if p.company_id == companies[0].id]
    company2_permissions = [p for p in final_permissions if p.company_id == companies[1].id]

    assert len(company1_permissions) == 1
    assert company1_permissions[0].role == Permission.RoleEnum.OPERATOR

    assert len(company2_permissions) == 1
    assert company2_permissions[0].role == Permission.RoleEnum.PAYROLL


def test_assign_permission_invited_user_flow(
    client: Client, session, owner_user_with_companies, force_session_mock
):
    owner_data = owner_user_with_companies
    companies = owner_data["companies"]
    owner_token = owner_data["token"]
    test_id = owner_data["test_id"]

    invited_email = f"invited-{test_id}@test.com"
    another_new_user_email = f"another-newuser-{test_id}@test.com"

    request_body = {
        "emails": [invited_email, another_new_user_email],
        "permissions": {
            str(companies[0].identifier): ["PAYROLL"],
            str(companies[1].identifier): ["OPERATOR"],
        },
    }

    response = client.http.put(
        "/Permission",
        body=json.dumps(request_body),
        headers={"Content-Type": "application/json", "access_token": owner_token},
    )

    assert response.status_code == 200
    response_data = response.json_body
    assert response_data["users_processed"] == 2  # Dos usuarios procesados
    assert response_data["companies_processed"] == 2

    invited_user_obj = session.query(User).filter(User.email == invited_email).first()
    assert invited_user_obj is not None
    assert invited_user_obj.invited_by_id == owner_data["user"].id

    invited_permissions = (
        session.query(Permission).filter(Permission.user_id == invited_user_obj.id).all()
    )

    assert len(invited_permissions) == 2  # Un permiso por cada empresa

    another_new_user = session.query(User).filter(User.email == another_new_user_email).first()
    assert another_new_user is not None
    assert another_new_user.invited_by_id == owner_data["user"].id

    another_permissions = (
        session.query(Permission).filter(Permission.user_id == another_new_user.id).all()
    )

    assert len(another_permissions) == 2


@pytest.mark.skip(reason="Comportamiento se valida en frontend, no en backend")
def test_invited_user_cannot_be_invited_to_different_workspace(
    client: Client, session, owner_user_with_companies
):
    pass


def test_owner_can_be_invited_to_other_workspaces(
    client: Client,
    session,
    owner_user_with_companies,
    other_owner_user_with_companies,
    force_session_mock,
):
    owner1_data = owner_user_with_companies
    owner1_user = owner1_data["user"]
    owner1_email = owner1_data["email"]
    owner1_companies = owner1_data["companies"]

    owner2_data = other_owner_user_with_companies
    owner2_user = owner2_data["user"]
    owner2_token = owner2_data["token"]
    owner2_companies = owner2_data["companies"]

    assert owner1_user.invited_by_id is None
    assert owner2_user.invited_by_id is None

    owner1_workspace = session.query(Workspace).filter(Workspace.owner_id == owner1_user.id).first()
    owner2_workspace = session.query(Workspace).filter(Workspace.owner_id == owner2_user.id).first()
    assert owner1_workspace is not None
    assert owner2_workspace is not None
    assert owner1_workspace.id != owner2_workspace.id

    request_body = {
        "emails": [owner1_email],
        "permissions": {
            str(owner2_companies[0].identifier): ["OPERATOR"],
            str(owner2_companies[1].identifier): ["PAYROLL"],
        },
    }

    response = client.http.put(
        "/Permission",
        body=json.dumps(request_body),
        headers={"Content-Type": "application/json", "access_token": owner2_token},
    )

    assert response.status_code == 200
    response_data = response.json_body
    assert response_data["users_processed"] == 1
    assert response_data["companies_processed"] == 2

    session.refresh(owner1_user)
    assert owner1_user.invited_by_id is None  # Debe mantenerse como None (owner original)

    owner1_permissions_in_owner2_workspace = (
        session.query(Permission)
        .filter(
            Permission.user_id == owner1_user.id,
            Permission.company_id.in_([c.id for c in owner2_companies]),
        )
        .all()
    )

    assert (
        len(owner1_permissions_in_owner2_workspace) == 2
    )  # OPERATOR en company1, PAYROLL en company2

    company1_perm = [
        p for p in owner1_permissions_in_owner2_workspace if p.company_id == owner2_companies[0].id
    ]
    company2_perm = [
        p for p in owner1_permissions_in_owner2_workspace if p.company_id == owner2_companies[1].id
    ]

    assert len(company1_perm) == 1
    assert company1_perm[0].role == Permission.RoleEnum.OPERATOR

    assert len(company2_perm) == 1
    assert company2_perm[0].role == Permission.RoleEnum.PAYROLL

    owner1_original_permissions = (
        session.query(Permission)
        .filter(
            Permission.user_id == owner1_user.id,
            Permission.company_id.in_([c.id for c in owner1_companies]),
        )
        .all()
    )

    assert len(owner1_original_permissions) >= 2  # Al menos los permisos originales

    session.refresh(owner1_workspace)
    assert owner1_workspace.owner_id == owner1_user.id

    session.refresh(owner2_workspace)
    assert owner2_workspace.owner_id == owner2_user.id


def test_invite_user_when_max_emails_enroll_reached(
    client: Client, session, owner_user_with_companies, force_session_mock
):
    owner_data = owner_user_with_companies
    owner_user = owner_data["user"]
    owner_token = owner_data["token"]
    companies = owner_data["companies"]
    test_id = owner_data["test_id"]

    workspace = owner_user.workspace
    workspace.license = {
        "id": 1,
        "date_start": "2024-07-03",
        "date_end": "2027-07-03",
        "details": {
            "max_emails_enroll": 2,  # Solo owner + 1 invitado permitido
            "max_companies": 50,
            "exceed_metadata_limit": False,
            "add_enabled": True,
            "products": [{"identifier": "prod_test", "quantity": 1}],
        },
        "stripe_status": "active",
    }
    session.add(workspace)
    session.commit()

    first_invited_email = f"first-invited-{test_id}@test.com"
    second_invited_email = f"second-invited-{test_id}@test.com"

    request_body_first = {
        "emails": [first_invited_email],
        "permissions": {str(companies[0].identifier): ["OPERATOR"]},
    }

    response_first = client.http.put(
        "/Permission",
        body=json.dumps(request_body_first),
        headers={"Content-Type": "application/json", "access_token": owner_token},
    )

    assert response_first.status_code == 200
    response_data_first = response_first.json_body
    assert response_data_first["users_processed"] == 1
    assert response_data_first["companies_processed"] == 1

    first_user = session.query(User).filter(User.email == first_invited_email).first()
    assert first_user is not None
    assert first_user.invited_by_id == owner_user.id

    request_body_second = {
        "emails": [second_invited_email],
        "permissions": {str(companies[1].identifier): ["PAYROLL"]},
    }

    response_second = client.http.put(
        "/Permission",
        body=json.dumps(request_body_second),
        headers={"Content-Type": "application/json", "access_token": owner_token},
    )

    assert response_second.status_code >= 400, (
        f"Expected error but got: {response_second.status_code}"
    )
    assert response_second.status_code != 200, (
        f"Expected error but got success: {response_second.status_code}"
    )

    if hasattr(response_second, "json_body") and response_second.json_body:
        error_message = response_second.json_body
        error_str = str(error_message).lower()
        assert any(keyword in error_str for keyword in ["maximum", "limit", "enroll", "reached"]), (
            f"Error message does not indicate enrollment limit: {error_message}"
        )

    second_user = session.query(User).filter(User.email == second_invited_email).first()
    assert second_user is None, "Second user should not have been created due to enrollment limit"

    second_user_permissions = (
        session.query(Permission).join(User).filter(User.email == second_invited_email).all()
    )
    assert len(second_user_permissions) == 0, "No permissions should exist for the second user"

    first_user_permissions = (
        session.query(Permission).filter(Permission.user_id == first_user.id).all()
    )
    assert len(first_user_permissions) == 1  # Solo el permiso que se asignó exitosamente
    assert first_user_permissions[0].role == Permission.RoleEnum.OPERATOR


def test_invited_user_invited_by_id_reset_when_no_permissions(
    client: Client, session, owner_user_with_companies, force_session_mock
):
    owner_data = owner_user_with_companies
    owner_user = owner_data["user"]
    owner_token = owner_data["token"]
    companies = owner_data["companies"]
    test_id = owner_data["test_id"]

    invited_email = f"invited-reset-test-{test_id}@test.com"

    request_body_assign = {
        "emails": [invited_email],
        "permissions": {
            str(companies[0].identifier): ["OPERATOR", "PAYROLL"],  # 2 permisos en company1
            str(companies[1].identifier): ["OPERATOR"],  # 1 permiso en company2
        },
    }

    response_assign = client.http.put(
        "/Permission",
        body=json.dumps(request_body_assign),
        headers={"Content-Type": "application/json", "access_token": owner_token},
    )

    assert response_assign.status_code == 200
    response_data = response_assign.json_body
    assert response_data["users_processed"] == 1
    assert response_data["companies_processed"] == 2

    invited_user = session.query(User).filter(User.email == invited_email).first()
    assert invited_user is not None
    assert invited_user.invited_by_id == owner_user.id  # Usuario fue invitado por el owner

    initial_permissions = (
        session.query(Permission).filter(Permission.user_id == invited_user.id).all()
    )
    assert len(initial_permissions) == 3  # 2 en company1 + 1 en company2

    request_body_remove_all = {
        "emails": [invited_email],
        "permissions": {
            str(companies[0].identifier): [],  # Array vacío = quitar todos los permisos
            str(companies[1].identifier): [],  # Array vacío = quitar todos los permisos
        },
    }

    response_remove = client.http.put(
        "/Permission",
        body=json.dumps(request_body_remove_all),
        headers={"Content-Type": "application/json", "access_token": owner_token},
    )

    assert response_remove.status_code == 200
    response_data_remove = response_remove.json_body
    assert response_data_remove["users_processed"] == 1
    assert response_data_remove["companies_processed"] == 2

    final_permissions = (
        session.query(Permission).filter(Permission.user_id == invited_user.id).all()
    )
    assert len(final_permissions) == 0  # Sin permisos

    session.refresh(invited_user)  # Refrescar desde BD
    assert invited_user.invited_by_id is None  # Debe ser NULL ahora


def test_owner_invited_by_id_never_changes(
    client: Client,
    session,
    owner_user_with_companies,
    other_owner_user_with_companies,
    force_session_mock,
):
    owner_a_data = owner_user_with_companies
    owner_a_user = owner_a_data["user"]
    owner_a_token = owner_a_data["token"]
    companies_a = owner_a_data["companies"]

    owner_b_data = other_owner_user_with_companies
    owner_b_user = owner_b_data["user"]
    owner_b_email = owner_b_data["email"]

    assert owner_a_user.invited_by_id is None
    assert owner_b_user.invited_by_id is None

    workspace_a = session.query(Workspace).filter(Workspace.owner_id == owner_a_user.id).first()
    workspace_b = session.query(Workspace).filter(Workspace.owner_id == owner_b_user.id).first()
    assert workspace_a is not None
    assert workspace_b is not None
    assert workspace_a.id != workspace_b.id

    request_body_invite = {
        "emails": [owner_b_email],
        "permissions": {str(companies_a[0].identifier): ["OPERATOR"]},
    }

    response_invite = client.http.put(
        "/Permission",
        body=json.dumps(request_body_invite),
        headers={"Content-Type": "application/json", "access_token": owner_a_token},
    )

    assert response_invite.status_code == 200

    session.refresh(owner_b_user)
    assert owner_b_user.invited_by_id is None  # NO debe cambiar porque es owner

    permissions_b = session.query(Permission).filter(Permission.user_id == owner_b_user.id).all()

    assert len(permissions_b) == 5

    permission_from_a = (
        session.query(Permission)
        .filter(Permission.user_id == owner_b_user.id, Permission.company_id == companies_a[0].id)
        .first()
    )
    assert permission_from_a is not None
    assert permission_from_a.role == Permission.RoleEnum.OPERATOR

    request_body_remove = {
        "emails": [owner_b_email],
        "permissions": {
            str(companies_a[0].identifier): []  # Array vacío = quitar todos los permisos
        },
    }

    response_remove = client.http.put(
        "/Permission",
        body=json.dumps(request_body_remove),
        headers={"Content-Type": "application/json", "access_token": owner_a_token},
    )

    assert response_remove.status_code == 200

    session.refresh(owner_b_user)
    assert owner_b_user.invited_by_id is None  # NO debe cambiar porque es owner

    permissions_b_after = (
        session.query(Permission).filter(Permission.user_id == owner_b_user.id).all()
    )

    assert len(permissions_b_after) == 4

    permission_from_a_after = (
        session.query(Permission)
        .filter(Permission.user_id == owner_b_user.id, Permission.company_id == companies_a[0].id)
        .first()
    )
    assert permission_from_a_after is None

    request_body_reassign = {
        "emails": [owner_b_email],
        "permissions": {
            str(companies_a[1].identifier): ["PAYROLL"]  # Diferente empresa y rol
        },
    }

    response_reassign = client.http.put(
        "/Permission",
        body=json.dumps(request_body_reassign),
        headers={"Content-Type": "application/json", "access_token": owner_a_token},
    )

    assert response_reassign.status_code == 200

    session.refresh(owner_b_user)
    assert owner_b_user.invited_by_id is None  # NO debe cambiar porque es owner

    permissions_b_final = (
        session.query(Permission).filter(Permission.user_id == owner_b_user.id).all()
    )
    assert len(permissions_b_final) == 5  # 4 originales + 1 nuevo

    permission_from_a_final = (
        session.query(Permission)
        .filter(Permission.user_id == owner_b_user.id, Permission.company_id == companies_a[1].id)
        .first()
    )
    assert permission_from_a_final is not None
    assert permission_from_a_final.role == Permission.RoleEnum.PAYROLL
