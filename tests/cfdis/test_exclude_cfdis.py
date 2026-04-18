from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.schema.models.tenant.cfdi import CFDI


def test_exclude_cfdis(company_session: Session):
    cfdi = CFDI.demo(ExcludeFromIVA=True, is_issued=False)

    company_session.add(cfdi)

    company_session.commit()

    body = {
        "company_identifier": cfdi.company_identifier,
        "cfdis": [
            {
                "UUID": cfdi.UUID,
                "ExcludeFromIVA": False,
                "is_issued": cfdi.is_issued,
            }
        ],
    }

    # Blueprint code

    company_identifier = body["company_identifier"]

    cfdis = body["cfdis"]

    data_to_update = {
        "company_identifier": company_identifier,
    }

    common_controller = CFDIController()

    common_controller.update_multiple(cfdis, company_session, data_to_update)

    data = company_session.query(CFDI).all()

    # Validamos que el cfdi ya no sea True como se había inicializado

    assert data[0].ExcludeFromIVA == False
