from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.controllers.tenant.session import new_company_session
from chalicelib.new.shared.infra.sqlalchemy_repo import SQLAlchemyRepo
from chalicelib.schema.models import ADDSyncRequest
from chalicelib.schema.models import Company as CompanyORM
from chalicelib.schema.models import PastoCompany as PastoCompanyORM
from chalicelib.schema.models import Workspace as WorkspaceORM
from chalicelib.schema.models.tenant import CFDI as CFDIORM


@dataclass
class LicenseRepositorySA(SQLAlchemyRepo):
    session: Session

    def get_workspace_from_license_key(self, license_key: str) -> WorkspaceORM:
        return (
            self.session.query(WorkspaceORM)
            .filter(WorkspaceORM.pasto_license_key == license_key)
            .first()
        )

    def get_companies_from_workspace_identifier(
        self, workspace_identifier: str
    ) -> list[CompanyORM]:
        companies_to_update = (
            self.session.query(CompanyORM)
            .filter(
                CompanyORM.workspace_identifier == workspace_identifier,
                CompanyORM.pasto_company_identifier.is_not(None),
            )
            .all()
        )

        self.session.query(CompanyORM).filter(
            CompanyORM.workspace_identifier == workspace_identifier,
            CompanyORM.pasto_company_identifier.is_not(None),
        ).update(
            {
                CompanyORM.pasto_company_identifier: None,
                CompanyORM.add_auto_sync: False,
                CompanyORM.pasto_last_metadata_sync: None,
            }
        )
        return companies_to_update

    def delete_related_company(self, workspace_identifier) -> None:
        self.session.query(PastoCompanyORM).filter(
            PastoCompanyORM.workspace_identifier == workspace_identifier
        ).delete()

    def update_table_cfdi(self, company_session: Session) -> None:
        company_session.query(CFDIORM).filter(
            CFDIORM.add_exists == True,
        ).update(
            {
                CFDIORM.add_exists: False,
                CFDIORM.add_cancel_date: None,
            }
        )

    def delete_data_from_add_sync_request(self, company_session: Session) -> None:
        company_session.query(ADDSyncRequest).delete()

    def update_on_db(self, license_key: str) -> None:
        workspace = self.get_workspace_from_license_key(license_key)
        companies = self.get_companies_from_workspace_identifier(workspace.identifier)
        for company in companies:
            with new_company_session(
                company.tenant_db_url, comment="Resetting license", read_only=False
            ) as company_session:
                self.update_table_cfdi(company_session)
                self.delete_data_from_add_sync_request(company_session)

        self.delete_related_company(workspace.identifier)
        workspace.pasto_installed = False
        self.session.commit()
