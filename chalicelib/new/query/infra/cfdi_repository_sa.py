from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import literal_column

from chalicelib.new.query.domain.enums import DownloadType
from chalicelib.new.query.infra import get_chunks_sa
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant.cfdi import CFDI

from .temp_table_sa import temp_table

XML_PROCESSED_FIELD = CFDIORM.Version.name  # TODO use a proper field

XML_FIELDS = tuple(
    {
        "Certificado",
        "CfdiRelacionados",
        "Conceptos",
        "CondicionesDePago",
        "Descuento",
        "DescuentoMXN",
        "DomicilioFiscalReceptor",
        "Exportacion",
        "Fecha",
        "FechaCertificacionSat",
        "FechaFiltro",
        "PaymentDate",
        "Folio",
        "FormaPago",
        "from_xml",
        "Impuestos",
        "LugarExpedicion",
        "Meses",
        "MetodoPago",
        "Moneda",
        "Neto",
        "NetoMXN",
        "NoCertificado",
        "NoCertificadoSAT",
        "NombreEmisor",
        "NombreReceptor",
        "Periodicidad",
        "RegimenFiscalEmisor",
        "RegimenFiscalReceptor",
        "RetencionesIEPS",
        "RetencionesIEPSMXN",
        "RetencionesISR",
        "RetencionesISRMXN",
        "RetencionesIVA",
        "RetencionesIVAMXN",
        "RfcEmisor",
        "RfcPac",
        "RfcReceptor",
        "Sello",
        "SelloSAT",
        "Serie",
        "SubTotal",
        "SubTotalMXN",
        "TipoCambio",
        "TipoDeComprobante_E_CfdiRelacionados_None",
        "TipoDeComprobante_E_MetodoPago_PPD",
        "TipoDeComprobante_I_MetodoPago_PPD",
        "TipoDeComprobante_I_MetodoPago_PUE",
        "TipoDeComprobante",
        "Total",
        "TotalMXN",
        "TrasladosIEPS",
        "TrasladosIEPSMXN",
        "TrasladosISR",
        "TrasladosISRMXN",
        "TrasladosIVA",
        "TrasladosIVAMXN",
        "UsoCFDIReceptor",
        "Version",
        "Year",
        "xml_content",
        "other_rfc",  # TODO remove
        "BaseIVA16",
        "BaseIVA8",
        "BaseIVA0",
        "BaseIVAExento",
        "IVATrasladado16",
        "IVATrasladado8",
        "ExcludeFromIVA",
        "ExcludeFromISR",
        "is_too_big",
    }
)
DUMMY_DEFAULT_FIELDS = tuple(
    {
        "company_identifier",
        "is_issued",
        "UUID",
        "Estatus",
        "cancelled_other_month",
        "active",
        "created_at",
        "updated_at",
    }
)
ALL_FIELDS = tuple(XML_FIELDS + DUMMY_DEFAULT_FIELDS)


@dataclass
class CFDIRepositorySA:  # TODO
    session: Session

    def get_chunks_need_xml(
        self,
        company_identifier: Identifier,
        download_type: DownloadType,
        max_per_chunk: int,
        start: datetime,
        end: datetime,
    ):
        return get_chunks_sa.get_chunks_need_xml(
            self.session,
            company_identifier,
            download_type,
            max_per_chunk,
            start=start,
            end=end,
        )

    def update_db_record_from_xml_memory_records(
        self,
        memory_records: Iterable[CFDIORM],
    ) -> dict[Identifier, bool]:
        temp_table_name = temp_table(
            session=self.session,
            name="tmp_cfdi",
            parent_table="cfdi",
            records=memory_records,
            fields=ALL_FIELDS,
        )

        select_tmp = select(*(literal_column(f'"{col}"') for col in ALL_FIELDS)).select_from(
            text(f'"{temp_table_name}"')
        )

        insert_stmt = insert(CFDI).from_select(ALL_FIELDS, select_tmp)

        update_dict = {f: literal_column(f'EXCLUDED."{f}"') for f in XML_FIELDS}

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["company_identifier", "is_issued", "UUID"],
            set_=update_dict,
            where=getattr(CFDI, XML_PROCESSED_FIELD).is_(None),
        ).returning(CFDI.UUID, CFDI.Estatus)

        result = self.session.execute(upsert_stmt)
        updated_rows = result.fetchall()

        return {str(row.UUID): row.Estatus for row in updated_rows}
