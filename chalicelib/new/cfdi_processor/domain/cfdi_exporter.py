import asyncio
import contextlib
import csv
import hashlib
import hmac
import io
import logging
import os
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any

from chalice import BadRequestError, NotFoundError
from sqlalchemy import and_, case, func, or_, select, text, union_all
from sqlalchemy.orm import Query, Session, aliased
from sqlalchemy.sql.elements import TextClause

from chalicelib.boto3_clients import s3_client
from chalicelib.controllers.cfdi import CFDIController, reset_group_by_and_having
from chalicelib.controllers.efos import NUMBER_TO_MONTH
from chalicelib.controllers.enums import ResumeType
from chalicelib.exceptions import DocDefaultException
from chalicelib.logger import DEBUG, ERROR, log, log_in
from chalicelib.modules import Modules
from chalicelib.new.cfdi_processor.domain.enums.cfdi_export_state import CfdiExportState
from chalicelib.new.cfdi_processor.domain.export import Export
from chalicelib.new.cfdi_processor.domain.query_to_export.pago_docs_relacionados import (
    pagos_column_types,
    query_pago_docs_relacionados,
)
from chalicelib.new.cfdi_processor.domain.xlsx_exporter import XLSXExporter, XLSXFields
from chalicelib.new.cfdi_processor.domain.xlsx_iva_fields import (
    exclusive_fields_per_section,
    include_fields_in_section,
)
from chalicelib.new.cfdi_processor.domain.xlsx_v2 import Export_iva, ExportV2
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import (
    CFDIExportRepositorySA,
)
from chalicelib.new.cfdi_processor.infra.messages.payload_message import SQSMessagePayload
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.log import set_global_logger
from chalicelib.new.isr import ISR_NAMES, ISRGetter
from chalicelib.new.iva import CREDITABLE_ISSUED, IVA_NAMES, IVAGetter
from chalicelib.new.query.domain.enums import DownloadType
from chalicelib.new.query.infra.copy_query import copy_query
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models import CfdiExport as CfdiExportORM
from chalicelib.schema.models.catalogs import CatFormaPago
from chalicelib.schema.models.tenant import CFDI, Payment
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant import DoctoRelacionado as DoctoRelacionadoORM
from chalicelib.schema.models.tenant.cfdi_export import CfdiExport as ExportRequestORM
from chalicelib.schema.models.tenant.tenant_model import PER_TENANT_SCHEMA_PLACEHOLDER

set_global_logger(logging.getLogger(__name__))


@dataclass
class ExportRepositoryS3:
    export_expiration: int = int(timedelta(weeks=1).total_seconds())
    s3_path: str = "Export"

    def save(self, export_bytes: bytes, export_metadata: CfdiExportORM, export_data: dict):
        file_name = self.get_file_name(export_metadata, export_data)
        s3_key = self.upload_to_s3_object(file_name, export_bytes)

        # get_s3_url
        if not s3_key:
            export_metadata.state = CfdiExportState.ERROR
            raise NotFoundError("Error uploading file to S3")  # TODO
        public_s3_url = self.get_public_s3_url(s3_key)
        export_metadata.url = public_s3_url
        export_metadata.state = CfdiExportState.TO_DOWNLOAD
        export_metadata.expiration_date = date.today() + timedelta(days=7)

    def get_s3_key(self, file_name: str) -> str:
        return os.path.join(self.s3_path, file_name)

    def upload_to_s3_object(self, file_name, data_bytes) -> str:
        try:
            key = self.get_s3_key(file_name)
            s3_client().upload_fileobj(
                io.BytesIO(data_bytes),
                envars.S3_EXPORT,
                key,
            )
            return key
        except Exception as e:
            log(
                Modules.EXPORT,
                ERROR,
                "S3_UPLOAD_ERROR",
                {
                    "file_name": file_name,
                    "error": str(e),
                },
            )
        return None

    def get_public_s3_url(self, key) -> str:
        access_key = envars.S3_ACCESS_KEY
        secret_key = envars.S3_SECRET_KEY
        region = envars.REGION_NAME
        service = "s3"

        bucket_name = envars.S3_EXPORT
        object_key = key
        url_base = f"https://{bucket_name}.s3.amazonaws.com/{object_key}"

        # days for expire url in seconds
        expires_in_seconds = 604800

        now = datetime.now(UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        canonical_request = (
            f"GET\n"
            f"/{object_key}\n"
            f"X-Amz-Algorithm=AWS4-HMAC-SHA256&"
            f"X-Amz-Credential={access_key}%2F{date_stamp}%2F{region}%2F{service}%2Faws4_request&"
            f"X-Amz-Date={amz_date}&X-Amz-Expires={expires_in_seconds}&X-Amz-SignedHeaders=host\n"
            f"host:{bucket_name}.s3.amazonaws.com\n\n"
            f"host\n"
            f"UNSIGNED-PAYLOAD"
        )

        canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

        string_to_sign = (
            f"AWS4-HMAC-SHA256\n{amz_date}\n"
            f"{date_stamp}/{region}/s3/aws4_request\n"
            f"{canonical_request_hash}"
        )

        def sign(key, msg):
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        key = ("AWS4" + secret_key).encode("utf-8")
        date_key = sign(key, date_stamp)
        region_key = sign(date_key, region)
        service_key = sign(region_key, service)
        signing_key = sign(service_key, "aws4_request")

        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        presigned_url = (
            f"{url_base}"
            f"?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            f"&X-Amz-Credential={access_key}%2F{date_stamp}%2F{region}%2F{service}%2Faws4_request"
            f"&X-Amz-Date={amz_date}"
            f"&X-Amz-Expires={expires_in_seconds}"
            f"&X-Amz-SignedHeaders=host"
            f"&X-Amz-Signature={signature}"
        )

        return presigned_url

    def get_file_name(self, export_metadata: CfdiExportORM, export_data: dict) -> str:
        if export_metadata.format == "XLSX":
            extension = "xlsx"
        elif export_metadata.format == "XML":
            extension = "zip"
            # if export_metadata.cfdis_qty == 1 else "zip"
        else:  # TODO enums can solve this
            raise ValueError("Invalid export format")
        file_name = export_data["file_name"]
        return f"{file_name}.{extension}"


@dataclass
class ExportInfo:
    uuids: list[Identifier] | None = None
    TipoDeComprobante: str | None = None  # | None TODO TipoDeComprobanteEnum
    download_type: DownloadType | None = None
    start: datetime | None = None
    end: datetime | None = None
    external_request: bool | None = None
    format: str | None = None  # TODO ExportFormatEnum


def complete_export_event_info_from_domain(domain: tuple[tuple[str, str, Any]]) -> ExportInfo:
    TipoDeComprobante = None
    download_type = None
    fechas = []
    uuids = []
    for rule in domain:
        try:
            field, comparator, value = rule
        except ValueError:
            continue  # Skip malformed rules
        if field in ["FechaFiltro", "Fecha"]:
            fechas.append(datetime.fromisoformat(value))
        elif field == "TipoDeComprobante":
            TipoDeComprobante = value
        elif field == "download_type":
            download_type = DownloadType(value)
        elif field == "UUID" and comparator == "in":
            uuids = value
        elif field == "is_issued":
            download_type = (
                DownloadType.ISSUED if value in [True, "true"] else DownloadType.RECEIVED
            )
    return ExportInfo(
        uuids=uuids,
        TipoDeComprobante=TipoDeComprobante,
        download_type=download_type,
        start=min(fechas, default=None),
        end=max(fechas, default=None),
    )


def complete_export_event_info(json_body) -> ExportInfo:
    domain_no_doted = [t for t in json_body["domain"] if t[0].find(".") == -1]
    info = complete_export_event_info_from_domain(domain_no_doted)
    info.external_request = bool(json_body.get("external_request"))
    info.format = json_body.get("format")
    info.export_data = json_body.get("export_data", {})
    info.cfdi_type = cfdi_type_format(info)
    return info


def cfdi_type_format(info):
    export_type = info.export_data.get("type")

    if export_type in {"doctos", "conceptos"}:
        return info.TipoDeComprobante + info.export_data.get("type")

    return info.TipoDeComprobante


MAX_DELTA_TO_EXPORT = timedelta(days=366)


class ExportException(DocDefaultException, BadRequestError):
    """Base class for export exceptions"""


class MissingEndDateError(ExportException):
    """end is required for export"""


class MissingStartDateError(ExportException):
    """start is required for export"""


class TimeWindowExceededError(ExportException):
    """Time window for export exceeded the maximum allowed"""


class MissingTipoDeComprobanteError(ExportException):
    """TipoDeComprobante is required for export"""


class MissingDownloadTypeError(ExportException):
    """download_type is required for export"""


class MissingExportFormatError(ExportException):
    """export_format is required for export"""


def assert_no_missing_info(info: ExportInfo) -> None:
    # sourcery skip: assign-if-exp, de-morgan, reintroduce-else, swap-if-expression
    if info.uuids:
        return
    if not info.start:
        raise MissingStartDateError()
    if not info.end:
        raise MissingEndDateError()
    if (info.end - info.start) > MAX_DELTA_TO_EXPORT:
        raise TimeWindowExceededError()
    if not info.TipoDeComprobante:
        raise MissingTipoDeComprobanteError()
    if not info.download_type:
        raise MissingDownloadTypeError()
    if not info.format:
        raise MissingExportFormatError()


@dataclass
class CFDIExporter:
    company_session: Session
    cfdi_export_repo: CFDIExportRepositorySA
    export_repo_s3: ExportRepositoryS3 = field(default_factory=ExportRepositoryS3)
    bus: EventBus = field(default_factory=EventBus)

    def export_event(self, json_body: dict, company_identifier: Identifier):  # LEGACY
        info = complete_export_event_info(json_body)
        assert_no_missing_info(info)

        cfdi_export = Export(
            format=info.format,
            cfdi_type=info.cfdi_type,
            start=info.start,
            end=info.end,
            download_type=info.download_type,
            external_request=info.external_request,
            file_name=info.export_data.get("file_name"),
        )

        self.cfdi_export_repo.save(cfdi_export)
        dict_repr = {"cfdi_export_identifier": cfdi_export.identifier}
        json_body.update(dict_repr)
        self.bus.publish(
            EventType.USER_EXPORT_CREATED,
            SQSMessagePayload(json_body=json_body, company_identifier=company_identifier),
        )
        return dict_repr

    def publish_export(
        self,
        company_identifier: Identifier,
        period: datetime,
        displayed_name: str,
        export_filter: Any,
        export_data_type: ExportRequestORM.ExportDataType,
        format: str,
        is_issued: bool,
        is_yearly: bool,
        export_data: dict,
        json_body: str | None = None,
    ) -> str:
        if export_filter is not None:
            domain_text = str(export_filter.compile(compile_kwargs={"literal_binds": True}))
            domain_text = domain_text.replace("per_tenant.", "")
        else:
            domain_text = ""

        export_request = ExportRequestORM(
            start=period.isoformat(),
            displayed_name=displayed_name,
            domain=domain_text,
            export_data_type=export_data_type,
            format=format,
            download_type=(DownloadType.ISSUED if is_issued else DownloadType.RECEIVED).value,
            external_request=is_yearly,
            file_name=export_data["file_name"],
        )
        self.company_session.add(export_request)
        self.company_session.commit()
        event_data = {
            "identifier": export_request.identifier,
            "export_data": export_data,
            "json_body": json_body,
        }
        self.bus.publish(
            EventType.MASSIVE_EXPORT_CREATED,
            SQSMessagePayload(json_body=event_data, company_identifier=company_identifier),
        )
        return event_data

    def export_xlsxv2(self, body, context, query, fields, resume_type, export_data) -> bytes:
        extra_fields_conceptos = [
            f"@{field.split('Conceptos.')[1]}" for field in fields if field.startswith("Conceptos.")
        ]

        extra_fields_nomina = [
            field.replace("N.Complemento.", "@")
            for field in fields
            if field.startswith("N.Complemento.")
        ]

        extra_fields = extra_fields_conceptos + extra_fields_nomina

        fields = [field for field in fields if not field.startswith("Conceptos.")]
        fields = [field for field in fields if not field.startswith("N.Complemento.")]

        if extra_fields_conceptos:
            fields.append("Conceptos")

        if extra_fields_nomina:
            fields.append("xml_content_text")

        query = self.company_session.query()

        is_nomina_export = resume_type == ResumeType.N
        if export_data and export_data.get("type") == "doctos":
            query = query_pago_docs_relacionados(self.company_session, body["domain"])
            column_types_override = pagos_column_types
        else:
            # Fix: Para nóminas (TipoDeComprobante='N'), no usar agregación
            # porque la relación CFDI ↔ Nomina es 1-a-1 (uselist=False)
            #
            # Con aggregate=True, los campos de nómina se agregan con string_agg()
            # causando concatenación de fechas: "2025-09-01 00:00:00, 2025-09-01 00:00:00"
            # lo cual falla al parsear en xlsx_v2.py con ParserError
            #
            # Para otros tipos de CFDI (I, P, E), aggregate=True puede ser necesario
            # si tienen relaciones 1-a-muchos que requieren GROUP BY
            query = CFDIController.get_query(
                CFDIORM,
                fields,
                body,
                aggregate=not is_nomina_export,  # False para nóminas, True para otros
                sql_query=query,
            )
            column_types_override = None
        query = CFDIController.apply_domain(
            query=query,
            domain=body["domain"],
            fuzzy_search=body["fuzzy_search"],
            session=self.company_session,
        )
        order_by = CFDIController._get_default_order_by(session=self.company_session)
        is_doctos_export = bool(export_data and export_data.get("type") == "doctos")
        if order_by and not is_doctos_export:
            # Si estamos agregando (GROUP BY) y el campo de ordenamiento no está
            # en los campos seleccionados
            # necesitamos usar una función de agregación (MAX) para evitar errores de agrupamiento.
            # Esto es común con el default 'FechaFiltro' que no siempre se exporta.
            #
            # Para exportaciones tipo 'doctos' (pago_docs_relacionados) se omite este bloque
            # porque esa query ya define su propio ORDER BY y no tiene GROUP BY — aplicar
            # func.max() sobre ella provoca un GroupingError de PostgreSQL.
            is_aggregated = not is_nomina_export
            if is_aggregated and order_by == "FechaFiltro" and "FechaFiltro" not in fields:
                query = query.order_by(func.max(CFDIORM.FechaFiltro).desc().nullsfirst())
            else:
                query = CFDIController._apply_order_by(CFDIORM, order_by, query)
        export_v2 = ExportV2(self.company_session)
        return export_v2.export(
            query=query,
            body=body,
            extra_fields=extra_fields,
            resume_type=resume_type,
            column_types_override=column_types_override,
        )

    def export_iva_xlsx(self, cfdis, fields, totals, export_data, extra_pages=None) -> bytes:
        xlsx_exporter = XLSXExporter()
        extra = {}
        if totals and export_data.get("iva") != "OpeConTer":
            extra["Totales"] = totals
        if export_data.get("iva") == "OpeConTer" and extra_pages:
            extra.update(extra_pages)
        return xlsx_exporter.new_export(export_data, cfdis, fields, extra)

    def export_xml(self, body, context, query, fields, resume_type, export_type) -> bytes:
        query = query.filter(
            CFDI.xml_content.is_not(None),
        )
        query = query.with_entities(CFDI.UUID, CFDI.xml_content)
        query_str = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
        with NamedTemporaryFile(mode="wb", suffix=".csv") as temp_file:
            copy_query(self.company_session, query_str, temp_file)
            return self._export_csv_xmls_to_zip(temp_file.name)

    def _export_csv_xmls_to_zip(
        self,
        csv_file_path: str,
    ) -> bytes:
        with TemporaryDirectory() as temp_dir, open(csv_file_path) as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                uuid, xml_content = row
                with open(os.path.join(temp_dir, f"{uuid}.xml"), "w", encoding="utf-8") as f:
                    f.write(xml_content)

            return compress_dir_as_zip(temp_dir)

    def export(
        self, cfdi_export_identifier: Identifier, body, fields, context, resume_type, export_data
    ):
        export_metadata: CfdiExportORM = self.cfdi_export_repo.get_by_identifier(
            cfdi_export_identifier
        )

        # Filter out special fields that shouldn't be processed by SQLAlchemy
        # These fields will be processed separately in the export functions
        filtered_fields = list(
            {
                field
                for field in body.get("fields", [])
                if not field.startswith("N.Complemento.") and not field.startswith("Conceptos.")
            }
            - {
                "balance",
                "uuid_total_egresos_relacionados",
                "total_relacionados_single",
            }
        )

        body["fields"] = filtered_fields

        query: Query = CFDIController._search(  # pylint: disable=protected-access
            **body, lazzy=True, session=self.company_session
        )
        self.assert_has_records_to_export(query, export_metadata)

        function_to_export = {
            "XLSX": self.export_xlsxv2,
            "XML": self.export_xml,  # TODO TMP: Investigación sobre caidas de BD
        }.get(export_metadata.format)

        if not function_to_export:
            log(
                Modules.EXPORT,
                ERROR,
                "FORMAT_NOT_SUPPORTED",
                {
                    "export_metadata": export_metadata,
                },
            )
            return
        export_bytes = function_to_export(body, context, query, fields, resume_type, export_data)
        export_metadata.file_name = export_data["file_name"]
        self.export_repo_s3.save(export_bytes, export_metadata, export_data)

        self.cfdi_export_repo.save(export_metadata)

    def remove_filters_from_domain(self, domain_str: str) -> str:
        conditions = domain_str.split(" AND ")

        excluded_filters = [
            "cfdi.is_issued",
            'cfdi."Version"',
            'cfdi."ExcludeFromIVA"',
            'cfdi."Estatus"',
        ]

        filtered_conditions = [
            cond
            for cond in conditions
            if not any(excluded in cond for excluded in excluded_filters)
        ]

        cleaned_domain = " AND ".join(filtered_conditions)

        return text(cleaned_domain)

    def get_combined_report(self, company_session: Session, start_date, end_date):
        cfdi_alias = aliased(CFDI)
        pagos_alias = aliased(CFDI)
        # Conteo de documentos relacionados
        pr_query = (
            company_session.query(
                cfdi_alias.RfcEmisor.label("RFC emisor"),
                cfdi_alias.NombreEmisor.label("Emisor"),
                func.count(DoctoRelacionadoORM.UUID).label("Cantidad de CFDIs"),
                func.sum(DoctoRelacionadoORM.BaseIVA16).label("Base IVA 16%"),
                func.sum(DoctoRelacionadoORM.BaseIVA8).label("Base IVA 8%"),
                func.sum(DoctoRelacionadoORM.BaseIVA0).label("Base IVA 0%"),
                func.sum(DoctoRelacionadoORM.BaseIVAExento).label("Base IVA Exento"),
                func.sum(DoctoRelacionadoORM.IVATrasladado16).label("IVA 16%"),
                func.sum(DoctoRelacionadoORM.IVATrasladado8).label("IVA 8%"),
                func.sum(DoctoRelacionadoORM.RetencionesIVAMXN).label("Retenciones IVA"),
            )
            .outerjoin(
                cfdi_alias,
                and_(
                    DoctoRelacionadoORM.UUID_related == cfdi_alias.UUID,
                    DoctoRelacionadoORM.Estatus == True,
                ),
            )
            .join(
                pagos_alias,
                and_(
                    DoctoRelacionadoORM.UUID == pagos_alias.UUID,
                    pagos_alias.Version == "4.0",
                ),
            )
            .filter(
                DoctoRelacionadoORM.FechaPago.between(start_date, end_date),
                cfdi_alias.Estatus == True,
                cfdi_alias.is_issued == False,
                DoctoRelacionadoORM.ExcludeFromIVA == False,
            )
            .group_by(cfdi_alias.RfcEmisor, cfdi_alias.NombreEmisor)
        )

        # Conteo de CFDIs
        cfdi_query = (
            company_session.query(
                CFDI.RfcEmisor.label("RFC emisor"),
                CFDI.NombreEmisor.label("Emisor"),
                func.count(CFDI.UUID).label("Cantidad de CFDIs"),
                func.sum(CFDI.BaseIVA16).label("Base IVA 16%"),
                func.sum(CFDI.BaseIVA8).label("Base IVA 8%"),
                func.sum(CFDI.BaseIVA0).label("Base IVA 0%"),
                func.sum(CFDI.BaseIVAExento).label("Base IVA Exento"),
                func.sum(CFDI.IVATrasladado16).label("IVA 16%"),
                func.sum(CFDI.IVATrasladado8).label("IVA 8%"),
                func.sum(CFDI.RetencionesIVAMXN).label("Retenciones IVA"),
            )
            .filter(
                CFDI.MetodoPago == "PUE",
                CFDI.is_issued == False,
                CFDI.Version == "4.0",
                CFDI.Estatus == True,
                CFDI.PaymentDate.between(start_date, end_date),
                CFDI.TipoDeComprobante == "I",
                CFDI.ExcludeFromIVA == False,
            )
            .group_by(CFDI.RfcEmisor, CFDI.NombreEmisor)
        )

        # Hacer unión de ambas consultas
        union_query = union_all(pr_query.statement, cfdi_query.statement).alias("combined_data")

        # Consulta final que agrupa por RFC
        result_ingresos = (
            company_session.query(
                union_query.c["RFC emisor"],
                union_query.c["Emisor"],
                func.sum(union_query.c["Cantidad de CFDIs"]).label("Cantidad de CFDIs"),
                func.sum(union_query.c["Base IVA 16%"]).label("Base IVA 16%"),
                func.sum(union_query.c["Base IVA 8%"]).label("Base IVA 8%"),
                func.sum(union_query.c["Base IVA 0%"]).label("Base IVA 0%"),
                func.sum(union_query.c["Base IVA Exento"]).label("Base IVA Exento"),
                func.sum(union_query.c["IVA 16%"]).label("IVA 16%"),
                func.sum(union_query.c["IVA 8%"]).label("IVA 8%"),
                func.sum(union_query.c["Retenciones IVA"]).label("Retenciones IVA"),
            )
            .group_by(union_query.c["RFC emisor"], union_query.c["Emisor"])
            .order_by(func.sum(union_query.c["Cantidad de CFDIs"]).desc())
        )
        result_egresos = (
            company_session.query(
                CFDIORM.RfcEmisor.label("RFC emisor"),
                CFDIORM.NombreEmisor.label("Emisor"),
                func.count(CFDIORM.UUID).label("Cantidad de CFDIs"),
                func.sum(CFDIORM.BaseIVA16).label("Base IVA 16%"),
                func.sum(CFDIORM.BaseIVA8).label("Base IVA 8%"),
                func.sum(CFDIORM.BaseIVA0).label("Base IVA 0%"),
                func.sum(CFDIORM.BaseIVAExento).label("Base IVA Exento"),
                func.sum(CFDIORM.IVATrasladado16).label("IVA 16%"),
                func.sum(CFDIORM.IVATrasladado8).label("IVA 8%"),
                func.sum(CFDIORM.RetencionesIVAMXN).label("Retenciones IVA"),
            )
            .filter(
                CFDIORM.is_issued == False,
                CFDIORM.Version == "4.0",
                CFDIORM.Estatus == True,
                CFDIORM.PaymentDate.between(start_date, end_date),
                CFDIORM.TipoDeComprobante == "E",
                CFDIORM.ExcludeFromIVA == False,
            )
            .group_by(CFDIORM.RfcEmisor, CFDIORM.NombreEmisor)
        )

        return result_ingresos, result_egresos

    def apply_schema(self, domain: TextClause) -> TextClause:
        schema_uuid = self.company_session.bind._execution_options["schema_translate_map"][
            "per_tenant"
        ]
        return text(domain.text.replace(PER_TENANT_SCHEMA_PLACEHOLDER, f'"{schema_uuid}"'))

    def export_iva(self, export_request: CfdiExportORM, export_data):
        def period_month(period: datetime) -> str:
            return f"{NUMBER_TO_MONTH[period.month]} {period.year}"

        issued = export_request.download_type == DownloadType.ISSUED
        other_fields = (
            ["RfcReceptor", "NombreReceptor"] if issued else ["RfcEmisor", "NombreEmisor"]
        )

        iva_fields = [
            "Fecha",
            "PaymentDate",
            "UUID",
            "Serie",
            "Folio",
            *other_fields,
            "TipoDeComprobante",
            "UsoCFDIReceptor",
            "MetodoPago",
            "forma_pago_code",
            "BaseIVA16",
            "BaseIVA8",
            "BaseIVA0",
            "BaseIVAExento",
            "IVATrasladado16",
            "IVATrasladado8",
            "iva_acreditable",
            "RetencionesIVAMXN",  # Computed
            "Total",
        ]
        iva_fields_acreditable = [
            "Fecha de pago",
            "Fecha de emisión",
            "UUID",
            "Serie",
            "Folio",
            *other_fields,
            "Forma de pago código",
            "Forma de pago",
            "DR - Serie",
            "DR - Folio",
            "DR - Fecha de emisión",
            "DR - UUID",
            "DR - Uso de CFDI",
            "DR - Objeto de impuesto",
            "DR - Base IVA 16%",
            "DR - Base IVA 8%",
            "DR - Base IVA 0%",
            "DR - Base IVA Exento",
            "DR - IVA Acreditable 16%",
            "DR - IVA Acreditable 8%",
            "DR - IVA Acreditable Total",
            "DR - Retenciones IVA",
            "DR - Importe pagado",
        ]

        include_fields_in_section(
            export_request.displayed_name,
            iva_fields,
            exclusive_fields_per_section,
        )
        if (
            export_request.download_type == DownloadType.RECEIVED
            and "pago facturas de crédito" in export_request.displayed_name.lower()
        ):
            payments = aliased(Payment, name="payments")
            cfdi_origin = aliased(CFDIORM, name="cfdi_origin")
            filters = self.apply_schema(self.remove_filters_from_domain(export_request.domain))
            new_filters = and_(
                filters,
                cfdi_origin.Estatus,
                cfdi_origin.is_issued == False,
                cfdi_origin.Version == "4.0",
                DoctoRelacionadoORM.ExcludeFromIVA == False,
                # Filtros adicionales sobre el CFDI relacionado (UUID_related)
                # para que coincidan con los del search de DoctoRelacionado
                CFDIORM.TipoDeComprobante == "I",
                or_(
                    CFDIORM.from_xml == True,
                    CFDIORM.is_too_big == True,
                ),
                CFDIORM.Estatus == True,
            )
            query = (
                self.company_session.query(
                    payments.FechaPago.label("Fecha de pago"),
                    cfdi_origin.Fecha.label("Fecha de emisión"),
                    cfdi_origin.UUID,
                    cfdi_origin.Serie,
                    cfdi_origin.Folio,
                    cfdi_origin.RfcEmisor.label("RfcEmisor"),
                    cfdi_origin.NombreEmisor.label("NombreEmisor"),
                    payments.FormaDePagoP.label("Forma de pago código"),
                    CatFormaPago.name.label("Forma de pago"),
                    DoctoRelacionadoORM.Serie.label("DR - Serie"),
                    DoctoRelacionadoORM.Folio.label("DR - Folio"),
                    CFDIORM.Fecha.label("DR - Fecha de emisión"),
                    DoctoRelacionadoORM.UUID_related.label("DR - UUID"),
                    CFDIORM.UsoCFDIReceptor.label("DR - Uso de CFDI"),
                    DoctoRelacionadoORM.ObjetoImpDR.label("DR - Objeto de impuesto"),
                    DoctoRelacionadoORM.BaseIVA16.label("DR - Base IVA 16%"),
                    DoctoRelacionadoORM.BaseIVA8.label("DR - Base IVA 8%"),
                    DoctoRelacionadoORM.BaseIVA0.label("DR - Base IVA 0%"),
                    DoctoRelacionadoORM.BaseIVAExento.label("DR - Base IVA Exento"),
                    DoctoRelacionadoORM.IVATrasladado16.label("DR - IVA Acreditable 16%"),
                    DoctoRelacionadoORM.IVATrasladado8.label("DR - IVA Acreditable 8%"),
                    DoctoRelacionadoORM.TrasladosIVAMXN.label("DR - IVA Acreditable Total"),
                    DoctoRelacionadoORM.RetencionesIVAMXN.label("DR - Retenciones IVA"),
                    DoctoRelacionadoORM.ImpPagadoMXN.label("DR - Importe pagado"),
                )
                .select_from(DoctoRelacionadoORM)
                .outerjoin(
                    CFDIORM,
                    DoctoRelacionadoORM.UUID_related == CFDIORM.UUID,
                )
                .outerjoin(
                    cfdi_origin,
                    cfdi_origin.UUID == DoctoRelacionadoORM.UUID,
                )
                .outerjoin(
                    payments,
                    payments.identifier == DoctoRelacionadoORM.payment_identifier,
                )
                .outerjoin(CatFormaPago, payments.FormaDePagoP == CatFormaPago.code)
                .filter(new_filters)
            )
            iva_fields = iva_fields_acreditable
        elif export_data["iva"] == "OpeConTer":
            iva = IVAGetter(company_session=self.company_session)
            period_str = export_request.start
            period_date = datetime.fromisoformat(period_str).date()
            window_dates = iva.get_window_dates(period_date, False)

            query_ingresos, query_egresos = self.get_combined_report(
                company_session=self.company_session,
                start_date=window_dates.period_start,
                end_date=window_dates.period_end,
            )
            iva_fields = {
                "RFC emisor": "RFC emisor",
                "Emisor": "Emisor",
                "Cantidad de CFDIs": "Cantidad de CFDIs",
                "Base IVA 16%": "Base IVA 16%",
                "Base IVA 8%": "Base IVA 8%",
                "Base IVA 0%": "Base IVA 0%",
                "Base IVA Exento": "Base IVA Exento",
                "IVA 16%": "IVA 16%",
                "IVA 8%": "IVA 8%",
                "Retenciones IVA": "Retenciones IVA",
            }

            egresos_rows = []

            headers = list(iva_fields.keys())
            egresos_rows.append(headers)

            for row in query_egresos:
                egresos_rows.append(
                    [
                        getattr(row, "RFC emisor", ""),
                        getattr(row, "Emisor", ""),
                        getattr(row, "Cantidad de CFDIs", 0),
                        getattr(row, "Base IVA 16%", 0),
                        getattr(row, "Base IVA 8%", 0),
                        getattr(row, "Base IVA 0%", 0),
                        getattr(row, "Base IVA Exento", 0),
                        getattr(row, "IVA 16%", 0),
                        getattr(row, "IVA 8%", 0),
                        getattr(row, "Retenciones IVA", 0),
                    ]
                )

            extra_pages = {"Egresos": egresos_rows}

            # Exportar con ambas hojas
            export_bytes = self.export_iva_xlsx(
                query_ingresos, iva_fields, [], export_data, extra_pages
            )
            self.export_repo_s3.save(export_bytes, export_request, export_data)
            self.cfdi_export_repo.save(export_request)

            return export_request

        else:
            # Todo el IVA traslado necesita esta query
            subquery_payment = select(
                Payment.uuid_origin,
                Payment.FormaDePagoP,
                func.row_number()
                .over(
                    partition_by=[Payment.uuid_origin],
                    order_by=Payment.identifier.desc(),
                )
                .label("rn"),
            ).alias("subquery_payment")
            query: Query = (
                self.company_session.query(
                    CFDIORM.Fecha,
                    CFDIORM.PaymentDate,
                    CFDIORM.UUID,
                    CFDIORM.Serie,
                    CFDIORM.Folio,
                    (CFDIORM.RfcReceptor if issued else CFDIORM.RfcEmisor),
                    (CFDIORM.NombreReceptor if issued else CFDIORM.NombreEmisor),
                    CFDIORM.TipoDeComprobante,
                    CFDIORM.UsoCFDIReceptor,
                    CFDIORM.MetodoPago,
                    case(
                        (
                            (CFDIORM.TipoDeComprobante == "I") & (CFDIORM.MetodoPago == "PUE"),
                            CFDIORM.FormaPago,
                        ),
                        (CFDIORM.TipoDeComprobante == "E", CFDIORM.FormaPago),
                        else_=subquery_payment.c.FormaDePagoP,
                    ).label("forma_pago_code"),
                    CFDIORM.BaseIVA16,
                    CFDIORM.BaseIVA8,
                    CFDIORM.BaseIVA0,
                    CFDIORM.BaseIVAExento,
                    CFDIORM.IVATrasladado16,
                    CFDIORM.IVATrasladado8,
                    CFDIORM.TrasladosIVAMXN.label("iva_acreditable"),
                    CFDIORM.RetencionesIVAMXN,
                    CFDIORM.Total,
                    CFDIORM.pr_count,
                )
                .outerjoin(
                    subquery_payment,
                    and_(
                        subquery_payment.c.uuid_origin == CFDIORM.UUID,
                        subquery_payment.c.rn == 1,
                    ),
                )
                .filter(self.apply_schema(text(export_request.domain)))  # type: ignore
            )

        self.assert_has_records_to_export(query, export_request)
        period = datetime.fromisoformat(export_request.start)

        yearly = export_request.external_request

        iva_getter = IVAGetter(self.company_session)
        iva_values = iva_getter._get_iva(
            period,
            issued,
            yearly,
        )

        xlsx_fields = (
            "qty",
            "PaymentRelatedCount",
            "BaseIVA16",
            "BaseIVA8",
            "BaseIVA0",
            "BaseIVAExento",
            "IVATrasladado16",
            "IVATrasladado8",
            "total",
            "RetencionesIVAMXN",
        )

        if export_request.download_type == DownloadType.RECEIVED:
            xlsx_fields = tuple(field for field in xlsx_fields if field != "PaymentRelatedCount")

        headers = [""] + [XLSXFields[field] for field in xlsx_fields]

        totals = [
            headers,
            [IVA_NAMES["i_tra"]] + [iva_values["i_tra"][k] for k in xlsx_fields],
            [IVA_NAMES["p_tra"]] + [iva_values["p_tra"].get(k, 0) for k in xlsx_fields],
            [IVA_NAMES["totals"]]
            + [(iva_values["i_tra"][k] + iva_values["p_tra"].get(k, 0)) for k in xlsx_fields],
            [IVA_NAMES["credit_notes"]] + [iva_values["credit_notes"][k] for k in xlsx_fields],
        ]
        if issued == CREDITABLE_ISSUED:
            totals.extend(
                [
                    ["IVA"] + [""] * (len(headers) - 2) + [iva_values["total"]],
                ]
            )
        if "reasignado" in export_request.displayed_name.lower():
            totals = []
        if "no considerados" in export_request.displayed_name.lower():
            totals = []
        export_bytes = self.export_iva_xlsx(query, iva_fields, totals, export_data)
        self.export_repo_s3.save(export_bytes, export_request, export_data)

        self.cfdi_export_repo.save(export_request)

    def export_iva_v2(self, export_request: CfdiExportORM, export_data, body):
        export_iva = Export_iva(self.company_session)
        query, column_types = export_iva.generate_query_excluded_iva_and_all_iva(body)
        generated_excel = export_iva.export_iva(query, column_types, body)

        self.export_repo_s3.save(generated_excel, export_request, export_data)
        self.cfdi_export_repo.save(export_request)

    def export_isr(self, export_request: CfdiExportORM, export_data):
        def period_month(period: datetime) -> str:
            return f"{NUMBER_TO_MONTH[period.month]} {period.year}"

        def period_month_or_year(period: datetime, yearly) -> str:
            return str(period.year) if yearly else period_month(period)

        issued = export_request.download_type == DownloadType.ISSUED

        other_fields = (
            ["RfcReceptor", "NombreReceptor"] if issued else ["RfcEmisor", "NombreEmisor"]
        )

        isr_fields = [
            "Fecha",
            "PaymentDate",
            "UUID",
            "Serie",
            "Folio",
            *other_fields,
            "TipoDeComprobante",
            "MetodoPago",
            "forma_pago_code",
            "BaseIVA16",
            "BaseIVA8",
            "BaseIVA0",
            "BaseIVAExento",
            "base_isr",  # Computed
            "RetencionesISRMXN",
        ]

        query: Query = self.company_session.query(CFDIORM).filter(
            self.apply_schema(text(export_request.domain))
        )
        self.assert_has_records_to_export(query, export_request)
        period = datetime.fromisoformat(export_request.start)

        yearly = export_request.external_request

        isr_getter = ISRGetter(self.company_session)
        isr_values = isr_getter._get_isr(
            period,
            issued,
            yearly,
        )

        xlsx_total_fields = (
            "qty",
            "BaseIVA16",
            "BaseIVA8",
            "BaseIVA0",
            "BaseIVAExento",
            "total",
            "RetencionesISRMXN",
        )
        headers = ["", "Período"] + [XLSXFields[field] for field in xlsx_total_fields]

        totals = [
            headers,
            [ISR_NAMES["invoice_pue"], period_month_or_year(period, yearly)]
            + [isr_values["invoice_pue"][k] for k in xlsx_total_fields],
            [ISR_NAMES["payments"], period_month_or_year(period, yearly)]
            + [isr_values["payments"][k] for k in xlsx_total_fields],
        ]

        if "reasignado" in export_request.displayed_name.lower():
            totals = []
        if "no considerados" in export_request.displayed_name.lower():
            totals = []

        export_bytes = self.export_iva_xlsx(query, isr_fields, totals, export_data)
        self.export_repo_s3.save(export_bytes, export_request, export_data)

        self.cfdi_export_repo.save(export_request)

    def handle_export_type(
        self,
        cfdi_export_identifier: Identifier,
        export_data: str,
        body: dict[str, Any],
    ):
        export_request: CfdiExportORM = self.cfdi_export_repo.get_by_identifier(
            cfdi_export_identifier
        )
        export_data = export_data
        export_type = export_request.export_data_type
        if export_type == CfdiExportORM.ExportDataType.IVA:
            export_data["iva"] = body.get("iva")
        if export_type == CfdiExportORM.ExportDataType.ISR:
            export_data["isr"] = body.get("isr")

        log(
            Modules.EXPORT,
            DEBUG,
            "EXPORT_IVA/ISR",
            {
                "cfdi_export_identifier": cfdi_export_identifier,
                "export_type": export_type,
                "body": body,
            },
        )
        if export_type == CfdiExportORM.ExportDataType.IVA:
            if body["issued"] is False and body["iva"] in {"all", "excluded"}:
                self.export_iva_v2(
                    export_request=export_request, export_data=export_data, body=body
                )
                return
            self.export_iva(export_request, export_data)
        elif export_type == CfdiExportORM.ExportDataType.ISR:
            self.export_isr(export_request, export_data)

    async def save_in_temp(
        self,
        temp_dir: str,
        list_identifiers: list[Identifier],
    ) -> None:
        query = self.company_session.query(CFDI.UUID, CFDI.xml_content)
        log_in(list_identifiers)
        query = query.filter(
            CFDI.UUID.in_(list_identifiers),
        )

        for cfdi in query:
            with open(os.path.join(temp_dir, f"{cfdi.UUID}.xml"), "w", encoding="utf-8") as f:
                f.write(cfdi.xml_content)

    def chunk_by_limit(self, query, limit: int, body) -> list[list[str]]:
        query = self.company_session.query(CFDI.UUID)
        query = CFDIController.apply_domain(
            query,
            body["domain"],
            body["fuzzy_search"],
            session=self.company_session,
        )
        query = query.filter(
            CFDI.xml_content.is_not(None),
        )
        query_count = self.company_session.query(func.count())
        query_count = CFDIController.apply_domain(
            query_count,
            body["domain"],
            body["fuzzy_search"],
            session=self.company_session,
        )
        query_count = query_count.filter(
            CFDI.xml_content.is_not(None),
        )
        query_count = reset_group_by_and_having(query_count)
        query = reset_group_by_and_having(query)
        count = query_count.scalar()

        array_list_uuids = []
        total_uuids_list = count
        uuids_temp = []
        total_cfdis = 0
        for uuid in query:
            uuids_temp.append(uuid[0])
            total_cfdis = total_cfdis + 1
            if len(uuids_temp) in [limit, total_uuids_list] or total_cfdis == total_uuids_list:
                array_list_uuids.append(uuids_temp)
                uuids_temp = []
        return array_list_uuids

    def upload_to_s3(self, file_path, bucket):
        with contextlib.suppress(Exception):  # TODO handle exceptions
            file_name = os.path.basename(file_path)
            key = f"Export/{file_name}"
            s3_client().upload_file(file_path, bucket, key)
            return key

    def assert_has_records_to_export(self, query: Query, cfdi_export: Export):
        if query.first():
            return
        cfdi_export.url = "EMPTY"
        cfdi_export.state = CfdiExportState.TO_DOWNLOAD
        self.cfdi_export_repo.save(cfdi_export)
        raise NotFoundError("No records found")


def compress_dir_as_zip(path: str):
    f = io.BytesIO()
    with zipfile.ZipFile(f, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for root, _dirs, files in os.walk(path):
            for file in files:
                zip_file.write(
                    os.path.join(root, file),
                    file,
                )
    return f.getvalue()


async def async_xml_export(tasks):
    await asyncio.gather(*tasks)
