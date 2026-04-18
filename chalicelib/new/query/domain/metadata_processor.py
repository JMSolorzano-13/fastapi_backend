import os
import typing
from collections.abc import Iterable
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from typing import Protocol

from sqlalchemy import and_, select, table, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import column

from chalicelib.new.package.domain.package import Package
from chalicelib.new.package.domain.package_repository import PackageRepository
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.metadata import Metadata
from chalicelib.new.query.domain.query import Query
from chalicelib.new.query.domain.zip_processor import ZipProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.new.query.infra.query_repository_sa import QueryRepositorySA
from chalicelib.new.query.infra.temp_table_sa import (
    default_cfdi_transformations,
    records_to_csv,
    temp_table,
)
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.datetime import utc_now
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado
from chalicelib.schema.models.tenant.docto_relacionado import DoctoRelacionado
from chalicelib.schema.models.tenant.payment import Payment

CSV_COLUMNS = [
    "company_identifier",
    "is_issued",
    "UUID",
    "Fecha",
    "Total",
    "TipoDeComprobante",
    "RfcEmisor",
    "NombreEmisor",
    "RfcReceptor",
    "NombreReceptor",
    "RfcPac",
    "FechaCertificacionSat",
    "Estatus",
    "FechaCancelacion",
    "FechaFiltro",
    "PaymentDate",
    "TipoDeComprobante_I_MetodoPago_PPD",
    "TipoDeComprobante_I_MetodoPago_PUE",
    "TipoDeComprobante_E_MetodoPago_PPD",
    "TipoDeComprobante_E_CfdiRelacionados_None",
    "cancelled_other_month",
    "other_rfc",
    "created_at",
    "updated_at",
    "active",
    "from_xml",
]


class MetadataRepository(Protocol):
    def records_from_packages(self, packages: Iterable[str]) -> list[Metadata]:
        raise NotImplementedError

    def records_from_zip_path(self, zip_path: str) -> list[Metadata]:
        raise NotImplementedError


@dataclass
class MetadataRepositoryZip(ZipProcessor):
    package_repo: PackageRepository

    def records_from_zip_path(self, zip_path: str) -> list[Metadata]:  # TODO unify
        zip_path = zip_path.split("/")[-1].split(".")[0]
        package = self.package_repo.get_from_sat_uuid(zip_path)
        return self._get_metadata_records_from_package(package)

    def records_from_packages(self, packages: Iterable[str]) -> list[Metadata]:
        metadatas = []
        for package in self.package_repo.get_from_sat_uuids(packages):  # TODO async
            metadatas.extend(self._get_metadata_records_from_package(package))
        return metadatas

    def _get_metadata_file_path(self, dir_path: str) -> str:
        for file_path in os.listdir(dir_path):
            if file_path.endswith(".txt") and not file_path.endswith("_tercero.txt"):
                return os.path.join(dir_path, file_path)
        raise ValueError("No metadata file found")

    def _get_metadata_records_from_package(self, package: Package) -> list[Metadata]:
        with self.decompress_temporary_path(package) as dir_path:
            metadata_path = self._get_metadata_file_path(dir_path)
            return Metadata.from_txt(metadata_path)


@dataclass
class MetadataProcessor:
    cfdi_repo: CFDIRepositorySA
    metadata_repo: MetadataRepository
    query_repo: QueryRepositorySA
    bus: EventBus

    # Procesador de Scraper
    def process_zip(self, company_identifier: Identifier, zip_path: str, rfc: str) -> None:
        metadatas = self.metadata_repo.records_from_zip_path(zip_path)
        self._process_metadata(metadatas, company_identifier, rfc)

    def process(self, query: Query) -> None:
        metadatas = self.metadata_repo.records_from_packages(query.packages)
        self._process_metadata(metadatas, query.company_identifier, query.company_rfc)
        self.mark_as_processed(query)

    def _process_metadata(
        self, metadatas: list[Metadata], company_identifier: str, rfc: str
    ) -> None:
        with NamedTemporaryFile(suffix=".csv", mode="w", encoding="UTF-8") as file:
            tmp_csv = self._write_tmp_csv(metadatas, company_identifier, rfc, file)
            self._upsert_using_temp_table(tmp_csv)
        self._cancel_related_if_needed(metadatas)

    def _metadata_to_csv(
        self,
        metadatas: Iterable[Metadata],
        company_rfc: str,
        company_identifier: Identifier,
        tmp_file,
    ):
        """Convierte los objetos Metadata → CSV usando la función compartida."""

        def adapter(md: Metadata, field: str):
            """Permite acceder a los atributos de Metadata desde records_to_csv."""
            return getattr(md, field, None)

        def build_record(md: Metadata) -> dict:
            other_rfc = md.RfcReceptor if md.RfcEmisor == company_rfc else md.RfcEmisor
            return {
                "company_identifier": str(company_identifier),
                "is_issued": md.RfcEmisor == company_rfc,
                "UUID": md.Uuid,
                "Fecha": md.FechaEmision,
                "Total": md.Monto,
                "TipoDeComprobante": md.TipoDeComprobante,
                "RfcEmisor": md.RfcEmisor,
                "NombreEmisor": md.NombreEmisor,
                "RfcReceptor": md.RfcReceptor,
                "NombreReceptor": md.NombreReceptor,
                "RfcPac": md.RfcPac,
                "FechaCertificacionSat": md.FechaCertificacionSat,
                "Estatus": md.Estatus,
                "FechaCancelacion": md.FechaCancelacion,
                "FechaFiltro": md.FechaEmision,
                "PaymentDate": md.FechaEmision,
                "TipoDeComprobante_I_MetodoPago_PPD": False,
                "TipoDeComprobante_I_MetodoPago_PUE": False,
                "TipoDeComprobante_E_MetodoPago_PPD": False,
                "TipoDeComprobante_E_CfdiRelacionados_None": False,
                "cancelled_other_month": False,
                "other_rfc": other_rfc,
                "created_at": utc_now(),
                "updated_at": utc_now(),
                "active": True,
                "from_xml": False,
            }

        records = [build_record(md) for md in metadatas]
        records_to_csv(
            records,
            CSV_COLUMNS,
            tmp_file,
            transformations=default_cfdi_transformations(adapter),
            with_header=True,
        )

    def _write_tmp_csv(
        self,
        metadatas: list[Metadata],
        company_identifier: str,
        rfc: str,
        file: typing.IO[typing.Any],
    ) -> str:
        self._metadata_to_csv(metadatas, rfc, company_identifier, file)
        return file.name

    def _upsert_using_temp_table(self, csv_path: str) -> None:
        session: Session = self.cfdi_repo.session
        pg_conn = session.connection().connection  # ← 1.  conexión única

        # Temp-table
        tmp_tbl = temp_table(
            session=session,
            name="tmp_cfdi",
            parent_table="cfdi",
            fields=CSV_COLUMNS,
        )

        # COPY
        cols = ", ".join(f'"{c}"' for c in CSV_COLUMNS)
        copy_sql = f'COPY "{tmp_tbl}" ({cols}) FROM STDIN WITH CSV HEADER'
        with pg_conn.cursor() as cur, open(csv_path, encoding="utf-8") as fh:
            cur.copy_expert(copy_sql, fh)  # ← 2.  misma conexión

        # Mantener CFDI_TMP_COLUMNS de forma local, si se crea globalmente se
        # corre el riesgo de "reutilización" por otros cid
        CFDI_TMP_COLUMNS = {c: column(c, type_=CFDI.__table__.c[c].type) for c in CSV_COLUMNS}

        # CTE: Filtrar registros nuevos o que necesitan actualización
        tmp_table_ref = table(tmp_tbl, *CFDI_TMP_COLUMNS.values())

        new_or_updated = (
            select(*CFDI_TMP_COLUMNS.values())
            .select_from(tmp_table_ref)
            .outerjoin(
                CFDI,
                and_(
                    CFDI.company_identifier == CFDI_TMP_COLUMNS["company_identifier"],
                    CFDI.is_issued == CFDI_TMP_COLUMNS["is_issued"],
                    CFDI.UUID == CFDI_TMP_COLUMNS["UUID"],
                ),
            )
            .where(
                CFDI.UUID.is_(None)
                | (CFDI.Estatus.is_(True) & CFDI_TMP_COLUMNS["Estatus"].is_(False))
            )
        ).cte("new_or_updated")

        # INSERT con ON CONFLICT DO UPDATE
        insert_stmt = insert(CFDI).from_select(
            CSV_COLUMNS,
            select(*[new_or_updated.c[c] for c in CSV_COLUMNS]),
        )

        excluded = insert_stmt.excluded
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["company_identifier", "is_issued", "UUID"],
            set_={
                "Estatus": excluded.Estatus,
                "FechaCancelacion": excluded.FechaCancelacion,
                "updated_at": utc_now(),
            },
            where=and_(
                CFDI.Estatus.is_(True),
                excluded.Estatus.is_(False),
            ),
        )

        session.execute(upsert_stmt)
        session.commit()

    def _cancel_related_if_needed(self, metadatas: list[Metadata]) -> None:
        uuids_to_cancel = [md.Uuid for md in metadatas if not md.Estatus]
        if not uuids_to_cancel:
            return

        session = self.cfdi_repo.session
        related_models = [
            (DoctoRelacionado, DoctoRelacionado.UUID),
            (Payment, Payment.uuid_origin),
            (CfdiRelacionado, CfdiRelacionado.uuid_origin),
        ]
        for model, field in related_models:
            stmt = (
                update(model)
                .where(field.in_(uuids_to_cancel), model.Estatus.is_(True))
                .values(Estatus=False)
            )
            session.execute(stmt)

    def mark_as_processed(self, query: Query) -> None:
        self.query_repo.update(query, {"state": QueryState.PROCESSED})
