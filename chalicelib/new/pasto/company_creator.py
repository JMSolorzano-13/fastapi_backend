from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.logger import WARNING, log, log_in
from chalicelib.modules import Modules
from chalicelib.new.shared.domain.primitives import normalize_identifier
from chalicelib.new.workspace.domain.workspace import Workspace
from chalicelib.schema.models import PastoCompany as PastoCompanyORM


@dataclass
class PastoCompany:
    Id: str
    GuidAdd: str
    NombreEmpresa: str
    Alias: str
    RFC: str
    BDD: str
    Sistema: str


CompaniesData = list[dict[str, str]]


@dataclass
class CompanyCreator:
    session: Session

    def _build_pasto_companies(self, data: CompaniesData) -> Iterable[PastoCompany]:
        companies = []
        for company in data:
            if not all(
                (
                    company.get("Id"),
                    company.get("GuidAdd"),
                    company.get("NombreEmpresa"),
                    company.get("Alias"),
                    company.get("RFC"),
                    company.get("BDD"),
                    company.get("Sistema"),
                )
            ):
                log(
                    Modules.ADD,
                    WARNING,
                    "INCOMPLETE_COMPANY",
                    {
                        "company": company,
                    },
                )
                continue
            company["GuidAdd"] = normalize_identifier(company["GuidAdd"])
            companies.append(PastoCompany(**company))
        return companies

    def create(
        self, workspace: Workspace, worker_id: str, data: CompaniesData
    ) -> Iterable[PastoCompanyORM]:
        pasto_companies = self._build_pasto_companies(data)
        pasto_companies_by_id = {
            normalize_identifier(pasto_company.GuidAdd): pasto_company
            for pasto_company in pasto_companies
        }
        ids = {normalize_identifier(pasto_company.GuidAdd) for pasto_company in pasto_companies}

        current_pasto_companies = self.session.query(PastoCompanyORM.pasto_company_id).filter(
            PastoCompanyORM.workspace_identifier == workspace.identifier
        )
        current_pasto_ids = {
            normalize_identifier(pasto_company.pasto_company_id)
            for pasto_company in current_pasto_companies
        }
        to_delete = current_pasto_ids - ids
        to_create = ids - current_pasto_ids
        to_update = ids & current_pasto_ids
        log_in(to_delete)
        self.session.query(PastoCompanyORM).filter(
            PastoCompanyORM.pasto_company_id.in_(to_delete),
            PastoCompanyORM.workspace_identifier == workspace.identifier,
        ).delete()

        log_in(to_update)
        current_pasto_companies = self.session.query(PastoCompanyORM).filter(
            PastoCompanyORM.pasto_company_id.in_(to_update)
        )
        for current_pasto_company in current_pasto_companies:
            pasto_company = pasto_companies_by_id[current_pasto_company.pasto_company_id]
            current_pasto_company.name = pasto_company.NombreEmpresa
            current_pasto_company.alias = pasto_company.Alias
            current_pasto_company.rfc = pasto_company.RFC
            current_pasto_company.bdd = pasto_company.BDD
            current_pasto_company.system = pasto_company.Sistema

        new_companies = [
            PastoCompanyORM(
                pasto_company_id=normalize_identifier(pasto_companies_by_id[pasto_id].GuidAdd),
                name=pasto_companies_by_id[pasto_id].NombreEmpresa,
                alias=pasto_companies_by_id[pasto_id].Alias,
                rfc=pasto_companies_by_id[pasto_id].RFC,
                workspace_identifier=workspace.identifier,
                bdd=pasto_companies_by_id[pasto_id].BDD,
                system=pasto_companies_by_id[pasto_id].Sistema,
            )
            for pasto_id in to_create
        ]
        self.session.add_all(new_companies)
        return to_delete, to_create, to_update
