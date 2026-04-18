import os

from sqlalchemy.orm import Session

from chalicelib.new.query.domain.metadata import Metadata
from chalicelib.new.query.domain.metadata_processor import MetadataProcessor
from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from tests.load_data.metadata_generator import create_metadata

XMLS_PATH = "tests/load_data/xml_test"
METADATA = "tests/metadata"


def read_files_from_directory(directory_path: str) -> list[str]:
    xmls = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".xml"):
            full_path = os.path.join(directory_path, filename)
            with open(full_path, encoding="utf-8") as f:
                xmls.append(f.read())
    return xmls


def test_load_metadara_without_xml_vigente(
    company_other: Company, company_session_other: Session, benchmark
):
    assert company_session_other.query(CFDI).count() == 0

    xml_content = read_files_from_directory(XMLS_PATH)
    cfdi_repo = CFDIRepositorySA(company_session_other)

    # Primera metadata con estatus=1
    metadata_dir = create_metadata(
        xml_content,
        company_other.identifier,
        estatus="1",
    )
    metadata = Metadata.from_txt(metadata_dir)

    pm = MetadataProcessor(cfdi_repo=cfdi_repo, metadata_repo=None, query_repo=None, bus=None)

    pm._process_metadata(
        metadatas=metadata, company_identifier=company_other.identifier, rfc=company_other.rfc
    )

    cfdis = company_session_other.query(CFDI).all()

    for cfdi in cfdis:
        # Provienen de metadata
        assert cfdi.Estatus == True
        assert cfdi.RfcEmisor is not None
        assert cfdi.NombreEmisor is not None
        assert cfdi.RfcReceptor is not None
        # No Provienen de metadata
        assert cfdi.Folio is None
        assert cfdi.Serie is None
        assert cfdi.Moneda is None

    def process_xmls():
        processor = XMLProcessor(
            cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session_other
        )
        processor.process_xml_files(
            company_identifier=company_other.identifier,
            xmls_contents=xml_content,
            rfc=company_other.rfc,
        )
        company_session_other.commit()

    # Benchmark PRIMERA carga
    benchmark(process_xmls)

    cfdis = company_session_other.query(CFDI).all()

    for cfdi in cfdis:
        # Provienen de metadata
        assert cfdi.Estatus == True
        assert cfdi.RfcEmisor is not None
        assert cfdi.NombreEmisor is not None
        assert cfdi.RfcReceptor is not None
        # No Provienen de metadata
        assert cfdi.Folio is not None
        assert cfdi.Serie is not None
        assert cfdi.Moneda is not None


def test_load_metadara_without_xml_cancel(
    company_other: Company, company_session_other: Session, benchmark
):
    assert company_session_other.query(CFDI).count() == 0

    xml_content = read_files_from_directory(XMLS_PATH)
    cfdi_repo = CFDIRepositorySA(company_session_other)

    # Primera metadata con estatus=1
    metadata_dir = create_metadata(
        xml_content,
        company_other.identifier,
        estatus="0",
    )
    metadata = Metadata.from_txt(metadata_dir)

    pm = MetadataProcessor(cfdi_repo=cfdi_repo, metadata_repo=None, query_repo=None, bus=None)

    pm._process_metadata(
        metadatas=metadata, company_identifier=company_other.identifier, rfc=company_other.rfc
    )

    cfdis = company_session_other.query(CFDI).all()

    for cfdi in cfdis:
        # Provienen de metadata
        assert cfdi.Estatus == False
        assert cfdi.RfcEmisor is not None
        assert cfdi.NombreEmisor is not None
        assert cfdi.RfcReceptor is not None
        # No Provienen de metadata
        assert cfdi.Folio is None
        assert cfdi.Serie is None
        assert cfdi.Moneda is None

    def process_xmls():
        processor = XMLProcessor(
            cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session_other
        )
        processor.process_xml_files(
            company_identifier=company_other.identifier,
            xmls_contents=xml_content,
            rfc=company_other.rfc,
        )
        company_session_other.commit()

    # Benchmark PRIMERA carga
    benchmark(process_xmls)

    cfdis = company_session_other.query(CFDI).all()

    for cfdi in cfdis:
        # Provienen de metadata
        assert cfdi.Estatus == False
        assert cfdi.RfcEmisor is not None
        assert cfdi.NombreEmisor is not None
        assert cfdi.RfcReceptor is not None
        # No Provienen de metadata
        assert cfdi.Folio is not None
        assert cfdi.Serie is not None
        assert cfdi.Moneda is not None
