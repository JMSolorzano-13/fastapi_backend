from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from chalicelib.new.company.domain import Company
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.envars.special_rfcs import SPECIAL_COMPANIES_SCRAP_CRON
from chalicelib.new.query.domain.enums import RequestType, SATDownloadTechnology
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.sqlalchemy_repo import SQLAlchemyRepo
from chalicelib.new.utils.datetime import utc_now
from chalicelib.schema.models import Company as CompanyORM
from chalicelib.schema.models import Workspace as WorkspaceORM
from chalicelib.schema.models.tenant import SATQuery as SATQueryORM

MOCK_MAIL_PROVIDERS = {
    "mozmail.com",
    "duck.com",
}

COMPANIES_ACTIVE_FILTER = (
    CompanyORM.active,
    CompanyORM.have_certificates,
    CompanyORM.has_valid_certs,
    WorkspaceORM.is_active,
)

COMPANIES_TO_SYNC_FILTER = (
    *COMPANIES_ACTIVE_FILTER,
    or_(~CompanyORM.exceed_metadata_limit, CompanyORM.permission_to_sync),
)


white_listed_identifiers = set()
for allowed_identifiers in SPECIAL_COMPANIES_SCRAP_CRON.values():
    white_listed_identifiers.update(allowed_identifiers)
COMPANIES_TO_SCRAP_FILTER = (
    or_(
        and_(
            *COMPANIES_TO_SYNC_FILTER,
            ~CompanyORM.rfc.in_(list(SPECIAL_COMPANIES_SCRAP_CRON.keys())),
        ),
        CompanyORM.identifier.in_(list(white_listed_identifiers)),
    ),
)


ACTIVE_ADD_FILTER = (
    CompanyORM.active,
    CompanyORM.pasto_company_identifier.is_not(None),
    WorkspaceORM.is_active,
)

COMPANIES_WITH_NOTIFICATIONS_FILTER = (
    *COMPANIES_ACTIVE_FILTER,
    or_(
        and_(
            CompanyORM.emails_to_send_efos.is_not(None),
            CompanyORM.emails_to_send_efos != "[]",
        ),
        and_(
            CompanyORM.emails_to_send_errors.is_not(None),
            CompanyORM.emails_to_send_errors != "[]",
        ),
        and_(
            CompanyORM.emails_to_send_canceled.is_not(None),
            CompanyORM.emails_to_send_canceled != "[]",
        ),
    ),
)


@dataclass
class CompanyRepositorySA(SQLAlchemyRepo):
    session: Session
    _model = Company
    _model_orm = CompanyORM

    def get_companies_without_xml_request(self) -> list[Company]:
        max_time_without_cfdis = utc_now() - envars.control.MAX_HOURS_WITHOUT_METADATA_FOR_COMPLETE
        companies_active = self.get_companies(COMPANIES_ACTIVE_FILTER)
        companies_without_xml_request = []
        for company in companies_active:
            have_recent_cfdi_request = (
                self.session.query(SATQueryORM)
                .filter(
                    SATQueryORM.created_at > max_time_without_cfdis,
                    SATQueryORM.request_type == RequestType.CFDI.value,
                    SATQueryORM.technology == SATDownloadTechnology.WebService.value,
                )
                .first()
            )
            if have_recent_cfdi_request is None:
                companies_without_xml_request.append(company)
        return companies_without_xml_request

    def is_especial_by_identifier(self, company_identifier: Identifier) -> bool:
        company = self.get_by_identifier(company_identifier)
        return company.rfc in envars.SPECIAL_RFCS

    def _create_record_orm(self, model: Company) -> None:
        workspace = (
            self.session.query(WorkspaceORM)
            .filter(
                WorkspaceORM.identifier == model.workspace_identifier,
            )
            .one()
        )
        company_orm = CompanyORM(
            name=model.name,
            workspace_identifier=model.workspace_identifier,
            workspace_id=workspace.id,
            rfc=model.rfc,
            active=model.active,
            identifier=model.identifier,
            have_certificates=model.have_certificates,
            exceed_metadata_limit=model.exceed_metadata_limit,
            permission_to_sync=model.permission_to_sync,
            last_notification=model.last_notification,
            emails_to_send_efos=model.emails_to_send_efos,
            emails_to_send_errors=model.emails_to_send_errors,
            emails_to_send_canceled=model.emails_to_send_canceled,
            pasto_company_identifier=model.pasto_company_identifier,
        )
        self.session.add(company_orm)

    def _model_from_orm(self, record_orm: CompanyORM) -> Company:
        return Company(
            id=record_orm.id,
            name=record_orm.name,
            workspace_identifier=record_orm.workspace_identifier,
            workspace_id=record_orm.workspace_id,
            rfc=record_orm.rfc,
            active=record_orm.active,
            exceed_metadata_limit=record_orm.exceed_metadata_limit,
            permission_to_sync=record_orm.permission_to_sync,
            last_notification=record_orm.last_notification,
            have_certificates=record_orm.have_certificates,
            emails_to_send_efos=record_orm.emails_to_send_efos,
            emails_to_send_errors=record_orm.emails_to_send_errors,
            emails_to_send_canceled=record_orm.emails_to_send_canceled,
            pasto_company_identifier=record_orm.pasto_company_identifier,
        ).set_identifier(record_orm.identifier)

    def _update_orm(self, record_orm: CompanyORM, model: Company) -> None:
        record_orm.id = model.id
        record_orm.name = model.name
        record_orm.workspace_identifier = model.workspace_identifier
        record_orm.rfc = model.rfc
        record_orm.active = model.active
        record_orm.have_certificates = model.have_certificates
        record_orm.exceed_metadata_limit = model.exceed_metadata_limit
        record_orm.permission_to_sync = model.permission_to_sync
        record_orm.last_notification = model.last_notification
        record_orm.emails_to_send_efos = (model.emails_to_send_efos,)
        record_orm.emails_to_send_errors = (model.emails_to_send_errors,)
        record_orm.emails_to_send_canceled = (model.emails_to_send_canceled,)
        record_orm.pasto_company_identifier = model.pasto_company_identifier

    def count_companies(self, filter) -> int:
        return (
            self.session.query(CompanyORM)
            .join(WorkspaceORM, CompanyORM.workspace_identifier == WorkspaceORM.identifier)
            .filter(*filter)
            .count()
        )

    def get_companies(self, filter, offset: int = 0, limit: int | None = None) -> Iterable[Company]:
        companies_orm = (
            self.session.query(CompanyORM)
            .join(WorkspaceORM, CompanyORM.workspace_identifier == WorkspaceORM.identifier)
            .order_by(CompanyORM.created_at)
            .filter(*filter)
            .offset(offset)
        )
        if limit:
            companies_orm = companies_orm.limit(limit)
        return (self._model_from_orm(company_orm) for company_orm in companies_orm)

    def get_companies_in_workspace(self, workspace_identifier: str) -> Iterable[Company]:
        companies_orm = self.session.query(CompanyORM).filter(
            CompanyORM.workspace_identifier == workspace_identifier,
        )
        return (self._model_from_orm(company_orm) for company_orm in companies_orm)
