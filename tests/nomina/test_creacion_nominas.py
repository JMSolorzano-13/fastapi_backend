from sqlalchemy.orm import Session

from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import CFDI, Nomina
from tests.load_data.test_company_load import read_files_from_directory

XMLS_PATH = "tests/nomina/nominas_xmls"


def test_creacion_nominas(company_session: Session, company: Company):
    cfdi_repo = CFDIRepositorySA(session=company_session)
    xml_content = read_files_from_directory(XMLS_PATH)

    xmlProcessor = XMLProcessor(cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session)

    xmlProcessor.process_xml_files(
        company_identifier=company.identifier,
        xmls_contents=xml_content,
        rfc=company.rfc,
    )

    select_nomina = company_session.query(Nomina).all()
    select_cfdi = company_session.query(CFDI).all()

    assert len(select_nomina) == len(select_cfdi)
