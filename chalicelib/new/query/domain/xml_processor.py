import json
import os
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy import and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi_utils import parsers
from chalicelib.controllers.cfdi_utils.parsers import (
    CFDIException,
    NotSupportedCFDI,
)
from chalicelib.controllers.enums import FormaPago, UsoCFDI
from chalicelib.logger import DEBUG, ERROR, EXCEPTION, INFO, WARNING, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.package.domain.package_repository import PackageRepository
from chalicelib.new.query.domain.cfdi_parser import CFDIFromXMLParser, CFDIParserInvalidVersion
from chalicelib.new.query.domain.cfdi_to_dict import CFDIDictFromXMLParser
from chalicelib.new.query.domain.nomina_parser import NominaParser
from chalicelib.new.query.domain.query import Query
from chalicelib.new.query.domain.zip_processor import ZipProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.new.query.infra.temp_table_sa import select_multiple, update_multiple
from chalicelib.new.shared.domain.primitives import Identifier, normalize_identifier
from chalicelib.new.utils.datetime import utc_now
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant import DoctoRelacionado as DoctoRelacionadoORM
from chalicelib.schema.models.tenant import Payment as PaymentORM
from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado
from chalicelib.schema.models.tenant.nomina import Nomina

XML_CONTENT = str
CFDIIdentifierDoctosToApply = dict[Identifier, Iterable[DoctoRelacionadoORM]]


class XMLRepository(Protocol):
    def get_xmls_from_query(self, query: Query) -> Iterable[XML_CONTENT]:
        raise NotImplementedError

    def get_xmls_from_zip_path(self, zip_path: str) -> Iterable[XML_CONTENT]:
        raise NotImplementedError


@dataclass
class XMLRepositoryZip(ZipProcessor):
    package_repo: PackageRepository

    def get_xmls_from_query(self, query: Query) -> Iterable[XML_CONTENT]:
        for package in self.package_repo.get_from_sat_uuids(query.packages):
            with self.decompress_temporary_path(package) as dir_path:
                for file_path in os.listdir(dir_path):
                    if not file_path.endswith(".xml"):
                        continue
                    file_path_complete = os.path.join(dir_path, file_path)
                    try:
                        xml_content = read_file(file_path_complete)
                    except ValueError:
                        log(
                            Modules.PROCESS_XML,
                            ERROR,
                            "FAILED_READING_FILE",
                            {
                                "file_path": file_path,
                                "company_identifier": query.company_identifier,
                            },
                        )
                        continue
                    yield xml_content

    def get_xmls_from_zip_path(self, zip_path: str) -> Iterable[XML_CONTENT]:  # TODO unify
        zip_path = zip_path.split("/")[-1].split(".")[0]
        package = self.package_repo.get_from_sat_uuid(zip_path)
        with self.decompress_temporary_path(package) as dir_path:
            for file_path in os.listdir(dir_path):
                if not file_path.endswith(".xml"):
                    continue
                file_path_complete = os.path.join(dir_path, file_path)
                try:
                    xml_content = read_file(file_path_complete)
                except ValueError:
                    log(
                        Modules.PROCESS_XML,
                        ERROR,
                        "FAILED_READING_FILE",
                        {
                            "file_path": file_path,
                            "zip_path": zip_path,
                        },
                    )
                    continue
                yield xml_content


@dataclass
class XMLProcessor:
    cfdi_repo: CFDIRepositorySA  # TODO
    xml_repo: XMLRepository
    company_session: Session

    def process(self, query: Query) -> None:
        company_identifier = query.company_identifier

        log(
            Modules.PROCESS_XML,
            INFO,
            "PROCESSING_QUERY",
            {
                "company_identifier": company_identifier,
                "query": query,
            },
        )

        xmls = self.xml_repo.get_xmls_from_query(query)
        self.process_xml_files(
            company_identifier=company_identifier,
            xmls_contents=xmls,
            rfc=query.company_rfc,
        )

    def process_zip(
        self, company_identifier: Identifier, zip_path: str, rfc: str
    ) -> None:  # TODO unify
        log(
            Modules.PROCESS_XML,
            INFO,
            "PROCESSING_ZIP",
            {
                "company_identifier": company_identifier,
                "path": zip_path,
            },
        )

        xmls = self.xml_repo.get_xmls_from_zip_path(zip_path)
        self.process_xml_files(
            company_identifier=company_identifier,
            xmls_contents=xmls,
            rfc=rfc,
        )

    def process_xml_files(
        self,
        /,
        company_identifier: Identifier,
        xmls_contents: Iterable[XML_CONTENT],
        rfc: str,
    ) -> None:
        memory_records = self._generate_memory_records(
            company_identifier=company_identifier,
            xmls_contents=xmls_contents,
            rfc=rfc,
        )
        uuids_updated = self.update_cfdis(
            company_identifier,
            memory_records,
        )
        memory_records_updated = []
        for memory_record in memory_records:
            if memory_record.UUID not in uuids_updated:
                continue
            # Establece el `Estatus` desde lo que se tiene en la BD
            memory_record.Estatus = uuids_updated[memory_record.UUID]
            memory_records_updated.append(memory_record)

        cfdis_ingreso = self.get_cfdi_ingreso_uuids(memory_records_updated)

        # iva e isr
        self.exclude_existing_documents(cfdis_ingreso, company_identifier)

        doctos = self.generate_payments(memory_records_updated)
        if doctos:
            # iva e isr
            self.exclude_new_docto_relacionados(doctos, company_identifier)

        self.create_nomina(memory_records_updated, company_identifier)
        self.generate_cfdi_relations(company_identifier, memory_records_updated)

    def get_cfdi_ingreso_uuids(self, memory_records_updated):
        if not memory_records_updated:
            return set()
        uuids = {
            record.UUID
            for record in memory_records_updated
            if record.UsoCFDIReceptor not in UsoCFDI.bancarizadas()
            and record.TipoDeComprobante == "I"
        }
        return uuids

    def exclude_existing_documents(
        self, new_income_uuids: set[str], company_identifier: Identifier
    ) -> None:
        """
        Excluye DoctoRelacionados existentes en BD que apuntan a los nuevos CFDI de ingreso

        Args:
            new_income_uuids: Conjunto de UUIDs de los nuevos CFDI de ingreso
            company_identifier: company
        """
        if not new_income_uuids:
            return
        # EXCLUIR IVA
        update_multiple(
            dest_table=DoctoRelacionadoORM.__tablename__,
            key="UUID_related",
            field_types=[("UUID_related", "UUID")],
            records=({"UUID_related": uuid} for uuid in new_income_uuids),
            session=self.company_session,
            fields_to_update_hardcoded={"ExcludeFromIVA": True},
            where_clause=and_(
                or_(
                    ~DoctoRelacionadoORM.ExcludeFromIVA,
                    DoctoRelacionadoORM.ExcludeFromIVA.is_(None),
                ),
            ),
            schema_name=company_identifier,
        )
        # EXCLUIR ISR
        update_multiple(
            dest_table=DoctoRelacionadoORM.__tablename__,
            key="UUID_related",
            field_types=[("UUID_related", "UUID")],
            records=({"UUID_related": uuid} for uuid in new_income_uuids),
            session=self.company_session,
            fields_to_update_hardcoded={"ExcludeFromISR": True},
            where_clause=and_(
                or_(
                    ~DoctoRelacionadoORM.ExcludeFromISR,
                    DoctoRelacionadoORM.ExcludeFromISR.is_(None),
                ),
            ),
            schema_name=company_identifier,
        )

    def exclude_new_docto_relacionados(
        self,
        new_docto_relacionados: list[DoctoRelacionadoORM],
        company_identifier: Identifier,
    ) -> None:
        """
        Excluye los nuevos DoctoRelacionados si apuntan a CFDI de ingreso existentes

        Args:
            new_docto_relacionados: Lista de nuevos DoctoRelacionados creados
            company_idetifier: company
        """
        if not new_docto_relacionados:
            return

        uuids_related_to_check = {
            normalize_identifier(docto.UUID_related) for docto in new_docto_relacionados
        }
        existing_income_uuids = select_multiple(
            source_table=CFDIORM.__tablename__,
            key=("UUID", "UUID_related"),
            field_types=[("UUID_related", "UUID")],
            records=({"UUID_related": uuid} for uuid in uuids_related_to_check),
            session=self.company_session,
            columns_to_select=["UUID"],
            where_clause=and_(
                CFDIORM.TipoDeComprobante == "I",
                CFDIORM.UsoCFDIReceptor.notin_(UsoCFDI.bancarizadas()),
            ),
            schema_name=company_identifier,
        )

        def _exclude_by_income_not_bancarizado(
            docto: DoctoRelacionadoORM, existing_income_uuids: set[str]
        ) -> bool:
            return normalize_identifier(docto.UUID_related) in existing_income_uuids

        def _exclude_by_pago_not_bancarizado(docto: DoctoRelacionadoORM) -> bool:
            return docto.payment.FormaDePagoP not in FormaPago.bancarizadas()

        for docto in new_docto_relacionados:  # TODO usar update_multiple
            if _exclude_by_income_not_bancarizado(
                docto, existing_income_uuids
            ) or _exclude_by_pago_not_bancarizado(docto):
                docto.ExcludeFromIVA = True
                docto.ExcludeFromISR = True

    def create_nomina(
        self, memory_records_updated: Iterable[CFDIORM], company_identifier: Identifier
    ) -> None:
        session = self.cfdi_repo.session

        nominas_data = [
            NominaParser.parse(cfdi.Nominas, cfdi.UUID, company_identifier).__dict__
            for cfdi in memory_records_updated
            if getattr(cfdi, "Nominas", None)
        ]

        if not nominas_data:
            return

        for item in nominas_data:
            item.pop("_sa_instance_state", None)

        stmt = insert(Nomina).values(nominas_data)
        stmt = stmt.on_conflict_do_nothing(index_elements=["company_identifier", "cfdi_uuid"])
        session.execute(stmt)

    def _generate_memory_records(
        self,
        /,
        company_identifier: Identifier,
        xmls_contents: Iterable[XML_CONTENT],
        rfc: str,
    ) -> Iterable[CFDIORM]:
        memory_records = []
        now = utc_now()
        uuids_seen = set()
        for xml_content in xmls_contents:
            try:
                cfdi = CFDIFromXMLParser.cfdi_from_xml(xml_content, rfc)
                if cfdi.UUID in uuids_seen:
                    log(
                        Modules.PROCESS_XML,
                        WARNING,
                        "DUPLICATED_XML_IN_BATCH",
                        {
                            "uuid": cfdi.UUID,
                            "company_identifier": company_identifier,
                        },
                    )
                    continue
                uuids_seen.add(cfdi.UUID)
                cfdi.company_identifier = company_identifier
                self.add_info_non_in_xml(cfdi, now)
            except CFDIParserInvalidVersion:  # pylint: disable=broad-except
                # log(
                #     Modules.PROCESS_XML,
                #     WARNING,
                #     "INVALID_VERSION",
                #     {
                #         "cfdi_dict": e.cfdi_dict,
                #         "company_identifier": company.identifier,
                #         "version": e.version,
                #     },
                # )
                continue
            except (Exception, CFDIException) as e:
                log(
                    Modules.PROCESS_XML,
                    EXCEPTION,
                    "FAILED_PARSING",
                    {
                        "company_identifier": company_identifier,
                        "xml_content": xml_content,
                        "exception": e,
                    },
                )
                continue
            if len(cfdi.xml_content) > envars.MAX_FILE_SIZE_KB * 1024:
                log(
                    Modules.PROCESS_XML,
                    WARNING,
                    "CFDI_TOO_BIG",
                    {
                        "company_identifier": company_identifier,
                        "uuid": cfdi.UUID,
                        "size": len(cfdi.xml_content),
                    },
                )
            memory_records.append(cfdi)
        return memory_records

    def add_info_non_in_xml(
        self,
        cfdi: CFDIORM,
        now: datetime,
    ) -> None:
        cfdi.created_at = now
        cfdi.updated_at = now
        cfdi.from_xml = True

        # TODO: check, not always true. Canceled XMLs can be downloaded from Scraper
        cfdi.Estatus = True
        cfdi.cancelled_other_month = False

        cfdi.active = True

    def generate_cfdi_relations(
        self, company_identifier: str, memory_records_updated: Iterable[CFDIORM]
    ) -> None:
        company_session = self.cfdi_repo.session

        update_records = []
        new_uuid_origins = set()
        current_relations_map = defaultdict(set)  # {uuid_origin: {uuid_related}}

        for cfdi in memory_records_updated:
            uuid_origin = cfdi.UUID
            new_uuid_origins.add(uuid_origin)

            relaciones = json.loads(cfdi.CfdiRelacionados or "{}")
            current_uuids = set()

            for doc in relaciones:
                tipo_relacion = doc["@TipoRelacion"].strip()
                for rel in doc["CfdiRelacionado"]:
                    uuid_related = rel["@UUID"].strip()
                    current_uuids.add(uuid_related)

                    update_records.append(
                        {
                            "uuid_origin": uuid_origin,
                            "uuid_related": uuid_related,
                            "TipoRelacion": tipo_relacion,
                            "Estatus": cfdi.Estatus,
                            "is_issued": cfdi.is_issued,
                            "TipoDeComprobante": cfdi.TipoDeComprobante,
                            "updated_at": utc_now(),
                        }
                    )

            current_relations_map[uuid_origin] = current_uuids

        if update_records:
            update_multiple(
                dest_table=CfdiRelacionado.__tablename__,
                key=("uuid_origin", "uuid_related", "TipoRelacion"),
                field_types=[
                    ("uuid_origin", "UUID"),
                    ("uuid_related", "UUID"),
                    ("TipoRelacion", "VARCHAR"),
                    ("Estatus", "BOOLEAN"),
                    ("is_issued", "BOOLEAN"),
                    ("TipoDeComprobante", "VARCHAR"),
                    ("updated_at", "TIMESTAMP"),
                ],
                records=update_records,
                session=company_session,
                fields_to_update=["Estatus", "is_issued", "TipoDeComprobante", "updated_at"],
                schema_name=company_identifier,
            )

        existing_relations = (
            company_session.query(CfdiRelacionado.uuid_origin, CfdiRelacionado.uuid_related)
            .filter(
                CfdiRelacionado.uuid_origin.in_(new_uuid_origins),
            )
            .all()
        )

        existing_set = {(r.uuid_origin, r.uuid_related) for r in existing_relations}
        new_relations = [
            r for r in update_records if (r["uuid_origin"], r["uuid_related"]) not in existing_set
        ]

        if new_relations:
            company_session.bulk_insert_mappings(
                CfdiRelacionado,
                [
                    {
                        "uuid_origin": r["uuid_origin"],
                        "uuid_related": r["uuid_related"],
                        "TipoRelacion": r["TipoRelacion"],
                        "Estatus": r["Estatus"],
                        "is_issued": r["is_issued"],
                        "TipoDeComprobante": r["TipoDeComprobante"],
                        "company_identifier": company_identifier,
                        "created_at": utc_now(),
                    }
                    for r in new_relations
                ],
            )

    def generate_payments(self, memory_records: list[CFDIORM]) -> set[Identifier]:
        all_docto_relacionados = []
        updates = []

        for cfdi in memory_records:
            pago_list = getattr(
                cfdi,
                "pago_list",
                parsers.get_pago_list(CFDIDictFromXMLParser.get_dict_from_xml(cfdi.xml_content)),
            )
            if not pago_list:
                continue

            docto_relacionados = []
            for index, pago in enumerate(pago_list):
                payment = PaymentORM.from_dict(pago, cfdi)
                payment.index = index
                self.company_session.add(payment)

                if not pago.get("DoctoRelacionado"):
                    log(
                        Modules.PROCESS_XML,
                        WARNING,
                        "PAYMENT_NO_DOCTO_RELACIONADO",
                        {
                            "payment": pago,
                        },
                    )
                    continue

                docto_relacionado_list = DoctoRelacionadoORM.from_dicts(
                    pago["DoctoRelacionado"], payment
                )
                self.company_session.add_all(docto_relacionado_list)
                docto_relacionados.extend(docto_relacionado_list)

            updates.append({"UUID": cfdi.UUID, "pr_count": len(docto_relacionados)})
            all_docto_relacionados.extend(docto_relacionados)

        if updates:
            company_identifier = memory_records[0].company_identifier
            update_multiple(
                dest_table=CFDIORM.__table__.name,
                key="UUID",
                field_types=[
                    ("UUID", "UUID"),
                    ("pr_count", "INTEGER"),
                ],
                records=updates,
                session=self.company_session,
                fields_to_update=["pr_count"],
                schema_name=company_identifier,
            )

        return all_docto_relacionados

    def _handle_not_supported_cfdi(self, e: NotSupportedCFDI):
        log(
            Modules.PROCESS_XML,
            WARNING,
            "NOT_SUPPORTED_CFDI",
            {"exception": e},
        )

    def update_cfdis(
        self,
        company_identifier: Identifier,
        memory_records: Iterable[CFDIORM],
    ) -> dict[Identifier, bool]:
        log(
            Modules.PROCESS_XML,
            DEBUG,
            "UPDATING_CFDI",
            {
                "company_identifier": company_identifier,
                "qty": len(memory_records),
            },
        )
        return self.cfdi_repo.update_db_record_from_xml_memory_records(memory_records)


def read_file(path: str) -> str:
    with open(path, encoding="UTF-8") as f:
        return f.read()
