from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from sqlalchemy import desc, or_
from sqlalchemy.orm import Session

from chalicelib.new.cfdi_processor.domain.export import Export
from chalicelib.new.query.domain.enums import DownloadType
from chalicelib.new.shared.infra.sqlalchemy_repo import SQLAlchemyRepo
from chalicelib.schema.models import CfdiExport as CfdiExportORM


@dataclass
class CFDIExportRepositorySA(SQLAlchemyRepo):
    session: Session
    _model = Export
    _model_orm = CfdiExportORM

    def get_records_by_company(self, company_identifier: str) -> Iterable[CfdiExportORM]:
        # TODO ensure access
        today = date.today()
        query = self.session.query(CfdiExportORM).filter(
            or_(CfdiExportORM.expiration_date >= today, CfdiExportORM.expiration_date.is_(None)),
        )
        query = query.order_by(desc(CfdiExportORM.created_at))
        return query.all()

    def _create_record_orm(self, model: Export) -> None:
        objet_orm = CfdiExportORM(
            url=model.url,
            state=model.state,
            expiration_date=model.expiration_date,
            identifier=model.identifier,
            start=model.start,
            end=model.end,
            cfdi_type=model.cfdi_type,
            format=model.format,
            download_type=model.download_type.value if model.download_type else None,
            external_request=model.external_request,
            domain=model.domain,
        )
        self.session.add(objet_orm)

    def _model_from_orm(self, record_orm: CfdiExportORM) -> Export:
        return Export(
            url=record_orm.url,
            state=record_orm.state,
            expiration_date=record_orm.expiration_date,
            start=record_orm.start,
            end=record_orm.end,
            cfdi_type=record_orm.cfdi_type,
            format=record_orm.format,
            download_type=DownloadType(record_orm.download_type)
            if record_orm.download_type
            else None,
            export_data_type=record_orm.ExportDataType(record_orm.export_data_type),
            external_request=record_orm.external_request,
            domain=record_orm.domain,
            displayed_name=record_orm.displayed_name,
            file_name=record_orm.file_name,
        ).set_identifier(record_orm.identifier)

    def _update_orm(self, record_orm: CfdiExportORM, model: Export) -> None:
        record_orm.state = model.state
        record_orm.expiration_date = model.expiration_date
        record_orm.url = model.url
        record_orm.start = model.start
        record_orm.end = model.end
        record_orm.cfdi_type = model.cfdi_type
        record_orm.format = model.format
        record_orm.download_type = model.download_type.value if model.download_type else None
        record_orm.external_request = model.external_request
        record_orm.domain = model.domain
        record_orm.file_name = model.file_name
