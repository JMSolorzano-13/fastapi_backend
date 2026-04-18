from chalice import UnauthorizedError
from sqlalchemy.orm import Session

from chalicelib.controllers import ensure_list
from chalicelib.controllers.common import CommonController
from chalicelib.new.workspace.infra import WorkspaceRepositorySA
from chalicelib.schema.models import ADDSyncRequest, Company
from chalicelib.schema.models.workspace import Workspace


class ADDSyncRequestController(CommonController):
    model = ADDSyncRequest

    _order_by = model.created_at.key

    @classmethod
    @ensure_list
    def check_companies(cls, records: list[ADDSyncRequest], *, session: Session, context=None):
        user = context["user"]
        session.add(user)
        allowed_companies = CommonController.get_user_companies(user, session=session)
        company_identifiers = {company.identifier for company in allowed_companies}
        requested_company_identifiers = {company.workspace_identifier for company in records}
        if not_allowed_company_identifiers := requested_company_identifiers - company_identifiers:
            raise UnauthorizedError(f"Companies `{not_allowed_company_identifiers}` not allowed")

    @classmethod
    def enable_auto_sync(cls, company: Company, add_auto_state, session: Session):
        workspace_repo = WorkspaceRepositorySA(session)
        workspace = workspace_repo._search_by_identifier(company.workspace_identifier)
        if not cls.is_add_permission(workspace):
            return {"message": "You don't have permission to add auto sync"}, 403
        company.add_auto_sync = add_auto_state
        return {"add_auto_sync": company.add_auto_sync}, 200

    @staticmethod
    def is_add_permission(workspace: Workspace):
        return workspace.add_permission
