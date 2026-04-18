from chalice import UnauthorizedError
from sqlalchemy.orm import Session

from chalicelib.controllers import ensure_list
from chalicelib.controllers.common import CommonController
from chalicelib.schema.models import PastoCompany


class PastoCompanyController(CommonController):
    model = PastoCompany

    @classmethod
    @ensure_list
    def check_companies(cls, records: list[PastoCompany], *, session: Session, context=None):
        user = context["user"]
        session.add(user)
        allowed_companies = CommonController.get_user_companies(user, session=session)
        allowed_workspaces = {company.workspace for company in allowed_companies} | set(
            CommonController.get_owned_by(user, session=session)
        )
        allowed_workspace_identifiers = {workspace.identifier for workspace in allowed_workspaces}
        session.add_all(records)
        requested_workspaces = {company.workspace_identifier for company in records}
        if not_allowed_workspaces := requested_workspaces - allowed_workspace_identifiers:
            raise UnauthorizedError(f"Workspaces `{not_allowed_workspaces}` not allowed")
