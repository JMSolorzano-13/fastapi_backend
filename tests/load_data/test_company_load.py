import os

from sqlalchemy.orm import Session

from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI

XMLS_PATH = "tests/load_data/xmls"
METADATA = "tests/metadata"


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
    assert company_session.query(CFDI).count() == 0

    cfdi_repo = CFDIRepositorySA(company_session)
    xml_content = read_files_from_directory(XMLS_PATH)

    processor = XMLProcessor(cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session)

    processor.process_xml_files(
        company_identifier=company.identifier,
        xmls_contents=xml_content,
        rfc=company.rfc,
    )

    company_session.commit()

    assert company_session.query(CFDI).count() == len(xml_content)


# Flujo con metadata
def test_load_xmls_with_metadata(company: Company, company_session: Session, benchmark):
    # Verifica que la tabla esté vacía antes de iniciar
    assert company_session.query(CFDI).count() == 0

    # Paso 1: Creación de metadata (fuera del benchmark)
    xml_content = read_files_from_directory(XMLS_PATH)

    # Definir la función que quieres medir
    def process_xmls():
        cfdi_repo = CFDIRepositorySA(company_session)
        processor = XMLProcessor(
            cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session
        )
        processor.process_xml_files(
            company_identifier=company.identifier,
            xmls_contents=xml_content,
            rfc=company.rfc,
        )
        company_session.commit()

    # Ejecutar benchmark
    benchmark(process_xmls)

    # Validar que efectivamente se insertaron todos
    assert company_session.query(CFDI).count() == len(xml_content)
