from sqlalchemy.orm import Session

from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.docto_relacionado import DoctoRelacionado
from tests.load_data.test_company_load import read_files_from_directory

XMLS_PATH = "tests/isr/xmls"


def test_auto_exclusion_isr(company_session: Session, company: Company):
    cfdi_repo = CFDIRepositorySA(session=company_session)
    xml_content = read_files_from_directory(XMLS_PATH)

    xmlProcessor = XMLProcessor(cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session)

    # Setup: DoctoRelacionado existente → ingreso que vendrá en XML (UsoCFDI no bancarizado)
    payment_uuid_existing = "55555555-5555-4555-8555-555555555555"
    income_uuid_from_xml = "11111111-1111-4111-8111-111111111111"

    pre_payment = CFDI.demo(
        with_xml=False,
        company_identifier=company.identifier,
        is_issued=False,
        UUID=payment_uuid_existing,
        TipoDeComprobante="P",
        Estatus=True,
    )
    pre_docto = DoctoRelacionado.demo(
        company_identifier=company.identifier,
        is_issued=False,
        UUID=payment_uuid_existing,
        UUID_related=income_uuid_from_xml,
        MonedaDR="MXN",
        NumParcialidad=1,
    )

    company_session.add_all([pre_payment, pre_docto])
    company_session.flush()

    xmlProcessor.process_xml_files(
        company_identifier=company.identifier,
        xmls_contents=xml_content,
        rfc=company.rfc,
    )

    company_session.expire_all()

    doctos = company_session.query(DoctoRelacionado).all()
    doctos_by_key = {(str(d.UUID), str(d.UUID_related)): d for d in doctos}

    # DoctoRelacionado existente → nuevo ingreso no bancarizado: debe excluirse
    pre_docto_db = doctos_by_key.get((payment_uuid_existing, income_uuid_from_xml))
    assert pre_docto_db is not None
    assert bool(pre_docto_db.ExcludeFromISR) is True
    assert bool(pre_docto_db.ExcludeFromIVA) is True

    # DoctoRelacionado nuevo → nuevo ingreso: NO se excluye (solo se excluyen si apuntan a ingresos existentes)
    docto_44_to_11 = doctos_by_key.get(
        ("44444444-4444-4444-8444-444444444444", income_uuid_from_xml)
    )
    assert docto_44_to_11 is not None
    assert bool(docto_44_to_11.ExcludeFromISR) is False
    assert bool(docto_44_to_11.ExcludeFromIVA) is False

    # Verificar que todos los CFDIs se cargaron
    cfdis = company_session.query(CFDI).all()
    cfdi_uuids = {str(c.UUID) for c in cfdis}
    assert {
        "11111111-1111-4111-8111-111111111111",
        "44444444-4444-4444-8444-444444444444",
        payment_uuid_existing,
    }.issubset(cfdi_uuids)
