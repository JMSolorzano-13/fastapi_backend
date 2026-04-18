import uuid

from sqlalchemy.orm import Session

from chalicelib.schema.models.company import Company
from chalicelib.schema.models.permission import Permission
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace


def test_create_company_v2(session: Session):
    workspace = Workspace()
    user = User(email="")
    session.add(workspace)
    session.add(user)
    session.flush()
    for i in range(2):
        company = Company(
            id=40000 + i,
            identifier=str(uuid.uuid4()),
            name="Test Company",
            pasto_company_identifier=None,
            workspace_identifier=workspace.identifier,
            workspace_id=workspace.id,
        )

        permissionOperator = Permission(
            id=7000 + i,
            identifier=str(uuid.uuid4()),
            user_id=user.id,
            company_id=company.id,
            role=Permission.RoleEnum.OPERATOR,
        )
        permissionPayroll = Permission(
            id=9000 + i,
            identifier=str(uuid.uuid4()),
            user_id=user.id,
            company_id=company.id,
            role=Permission.RoleEnum.PAYROLL,
        )

        session.add_all([company, permissionOperator, permissionPayroll])
    session.commit()

    assert company.name == "Test Company"
