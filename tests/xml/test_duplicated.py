import pytest
from sqlalchemy.orm import Session

from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI

XML_PATH = "tests/load_data/xml_test/232b5f16-6cf5-4544-a27d-41a531a4550c.xml"


@pytest.fixture
def xml_path() -> str:
    return XML_PATH


@pytest.fixture
def xml_content(xml_path: str) -> str:
    with open(xml_path, encoding="utf-8") as f:
        return f.read()


@pytest.mark.parametrize("times", range(5))
def test_no_crash_if_duplicated(
    company: Company,
    company_session: Session,
    times: int,
    xml_content: str,
):
    company_identifier = company.identifier
    assert company_session.query(CFDI).filter_by(company_identifier=company_identifier).count() == 0

    cfdi_repo = CFDIRepositorySA(company_session)

    xml_contents = duplicate_string_n_times(times, xml_content)
    processor = XMLProcessor(
        cfdi_repo=cfdi_repo,
        xml_repo=None,  # type: ignore
        company_session=company_session,
    )
    processor.process_xml_files(
        company_identifier=company_identifier,
        xmls_contents=xml_contents,
        rfc=company.rfc,
    )


def duplicate_string_n_times(times: int, string: str) -> list[str]:
    return [string] * times
