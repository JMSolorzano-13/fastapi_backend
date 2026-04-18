from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado


def test_cfdi_cfdi_relacionado(company_session: Session, company: Company):
    cfdi_e = CFDI.demo(TipoDeComprobante="E", UUID="00000000-0000-0000-0000-000000000001")
    cfdi_i = CFDI.demo(TipoDeComprobante="I", UUID="00000000-0000-0000-0000-000000000002")
    relacion = CfdiRelacionado(
        company_identifier=company.identifier,
        uuid_origin=cfdi_e.UUID,
        TipoDeComprobante=cfdi_e.TipoDeComprobante,
        is_issued=cfdi_e.is_issued,
        Estatus=cfdi_e.Estatus,
        uuid_related=cfdi_i.UUID,
        TipoRelacion="03",
    )

    company_session.add_all([cfdi_e, cfdi_i, relacion])
    company_session.commit()

    assert cfdi_i.cfdi_related == [relacion]
    assert cfdi_e.cfdi_origin == [relacion]

    search_result_i = CFDIController._search(
        domain=[
            ("UUID", "=", cfdi_i.UUID),
        ],
        fields=[
            "UUID",
            "cfdi_related.uuid_origin",
        ],
        session=company_session,
    )
    assert search_result_i == [
        (
            cfdi_i.UUID,
            [
                {
                    "uuid_origin": cfdi_e.UUID,
                }
            ],
        )
    ]

    search_result_e = CFDIController._search(
        domain=[
            ("UUID", "=", cfdi_e.UUID),
        ],
        fields=[
            "UUID",
            "cfdi_origin.uuid_related",
            "cfdi_origin.TipoRelacion",
            "cfdi_origin.cfdi_related.TipoDeComprobante",
            "cfdi_origin.cfdi_related.Estatus",
            "cfdi_origin.cfdi_related.UUID",
        ],
        session=company_session,
    )
    assert search_result_e == [
        (
            cfdi_e.UUID,
            [
                {
                    "uuid_related": cfdi_i.UUID,
                    "TipoRelacion": relacion.TipoRelacion,
                    "cfdi_related.TipoDeComprobante": cfdi_i.TipoDeComprobante,
                    "cfdi_related.Estatus": cfdi_i.Estatus,
                    "cfdi_related.UUID": cfdi_i.UUID,
                }
            ],
        )
    ]
    search_e_nested = CFDIController.to_nested_dict(search_result_e)
    assert search_e_nested == [
        {
            "UUID": cfdi_e.UUID,
            "cfdi_origin": [
                {
                    "uuid_related": cfdi_i.UUID,
                    "TipoRelacion": relacion.TipoRelacion,
                    "cfdi_related": {
                        "TipoDeComprobante": cfdi_i.TipoDeComprobante,
                        "Estatus": cfdi_i.Estatus,
                        "UUID": cfdi_i.UUID,
                    },
                }
            ],
        }
    ]
