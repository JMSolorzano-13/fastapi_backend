from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.logger import log_in
from chalicelib.new.license.domain import LicenseDetails
from chalicelib.new.shared.infra.sqlalchemy_repo import SQLAlchemyRepo
from chalicelib.new.workspace.domain.workspace import Workspace
from chalicelib.schema.models import (
    Permission as PermissionORM,
)
from chalicelib.schema.models import (
    User as UserORM,
)
from chalicelib.schema.models import (
    Workspace as WorkspaceORM,
)


@dataclass
class LicenseRepositorySA(SQLAlchemyRepo):
    session: Session
    _model = Workspace
    _model_orm = WorkspaceORM

    def get_current_users_enrolled(self, workspace: WorkspaceORM) -> int:
        all_company_ids = {company.id for company in workspace.companies}
        log_in(all_company_ids)
        permissions = self.session.query(PermissionORM).filter(
            PermissionORM.company_id.in_(all_company_ids)  # TODO change `id` to `identifier`
        )
        return len({permission.user for permission in permissions})

    def get_current_companies_created(self, workspace: WorkspaceORM) -> int:
        return len(workspace.companies)

    def get_current_used_characteristics(self, workspace: Workspace) -> LicenseDetails:
        workspace_orm = self._search_by_identifier(workspace.identifier)
        return LicenseDetails(
            max_emails_enroll=self.get_current_users_enrolled(workspace_orm),
            max_companies=self.get_current_companies_created(workspace_orm),
        )

    def user_has_permission_to_modify(self, user: UserORM, workspace: Workspace) -> bool:
        self.session.add(user)
        workspace_orm = self._search_by_identifier(workspace.identifier)
        return user.workspace == workspace_orm
