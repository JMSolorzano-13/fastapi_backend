import os

import pytest
from sqlalchemy import func
from sqlalchemy.orm import Session

from chalicelib.new.query.domain.metadata import Metadata
from chalicelib.new.query.domain.metadata_processor import MetadataProcessor
from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI

XMLS_PATH = "tests/load_data/xmls"
METADATA_PATH = "tests/metadata/59080449-d89e-4713-b967-ccfa6e2a101b.txt"


def read_files_from_directory(directory_path: str) -> list[str]:
    xmls = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".xml"):
            full_path = os.path.join(directory_path, filename)
            with open(full_path, encoding="utf-8") as f:
                xmls.append(f.read())
    return xmls


# Flujo sin metadata
def test_load_xmls(company: Company, company_session: Session):
    company_identifier = company.identifier
    assert (
        company_session.query(CFDI).filter(CFDI.company_identifier == company_identifier).count()
        == 0
    )

    cfdi_repo = CFDIRepositorySA(company_session)
    xml_content = read_files_from_directory(XMLS_PATH)
    processor = XMLProcessor(cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session)
    processor.process_xml_files(
        company_identifier=company_identifier,
        xmls_contents=xml_content,
        rfc=company.rfc,
    )

    # Assert que valide que la misma cantidad de xml se agregaron a la base de datos
    assert company_session.query(CFDI).filter(
        CFDI.company_identifier == company_identifier
    ).count() == len(xml_content)
    # No se genera metadata aquí.


@pytest.fixture
def cfdi_data(company: Company, company_session: Session):
    if company_session.query(func.count()).select_from(CFDI).scalar() > 0:
        return  # Datos ya cargados
    cfdi_repo = CFDIRepositorySA(company_session)
    xml_content = read_files_from_directory(XMLS_PATH)
    processor = XMLProcessor(cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session)
    processor.process_xml_files(
        company_identifier=company.identifier,
        xmls_contents=xml_content,
        rfc=company.rfc,
    )


def test_data_loaded(
    company: Company,
    company_session: Session,
    cfdi_data,  # FIXTURE_LOADED
):
    assert company_session.query(func.count()).select_from(CFDI).scalar() > 0


# Flujo con metadata
def test_load_xmls_with_metadata(company: Company, company_session: Session):
    company_identifier = company.identifier
    assert (
        company_session.query(CFDI).filter(CFDI.company_identifier == company_identifier).count()
        == 0
    )

    cfdi_repo = CFDIRepositorySA(company_session)
    # Paso 1: Cargar metadata desde un archivo conocido del repo
    metadata = Metadata.from_txt(METADATA_PATH)
    pm = MetadataProcessor(cfdi_repo=cfdi_repo, metadata_repo=None, query_repo=None, bus=None)
    pm._process_metadata(metadatas=metadata, company_identifier=company_identifier, rfc=company.rfc)

    # Assert que valide que la misma cantidad de metadata se agregaron a la base de datos
    assert company_session.query(CFDI).filter(
        CFDI.company_identifier == company_identifier
    ).count() == len(metadata)
