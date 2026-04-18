from sqlalchemy.orm import Session

from chalicelib.controllers.docto_relacionado import DoctoRelacionadoController
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.docto_relacionado import DoctoRelacionado


def test_exclude_docto(company_session: Session, company: Company):
    docto = DoctoRelacionado.demo()
    company_session.add(docto)
    company_session.flush()
    json_body = {
        "company_identifier": company.identifier,
        "uuid_relations": {docto.identifier: True},
    }

    uuids = json_body["uuid_relations"]

    docto_controller = DoctoRelacionadoController()
    message = docto_controller.set_exclude_from_iva(uuids=uuids, session=company_session)

    assert message == {"result": "ok"}
    assert docto.ExcludeFromIVA == True
