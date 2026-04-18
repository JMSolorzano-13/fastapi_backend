from decimal import Decimal

from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.schema.models import Company
from chalicelib.schema.models.tenant.cfdi import CFDI as CFDIORM
from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado

SPECIAL_FIELDS = {"balance", "uuid_total_egresos_relacionados", "total_relacionados_single"}


def test_total_egresos_suma_todos(company_session: Session, company: Company):
    cid = company.identifier

    ingreso = CFDIORM.demo(
        company_identifier=cid,
        TipoDeComprobante="I",
        is_issued=False,
        is_received=True,
        Estatus=True,
    )
    company_session.add(ingreso)
    company_session.flush()

    # Dos egresos con el mismo TotalMXN para reproducir el bug de SUM(DISTINCT)
    # Un egreso en USD para verificar que se usa TotalMXN y no Total
    montos = [
        (5_000.00, 5_000.00),
        (8_687.90, 8_687.90),  # duplicado
        (8_687.90, 8_687.90),  # duplicado
        (10_000.00, 10_000.00),
        (200.00, 3_893.41),  # USD → TotalMXN distinto de Total
        (15_000.00, 15_000.00),
    ]

    for total, total_mxn in montos:
        egreso = CFDIORM.demo(
            company_identifier=cid,
            TipoDeComprobante="E",
            is_issued=True,
            is_received=False,
            Estatus=True,
            Total=total,
            TotalMXN=total_mxn,
        )
        company_session.add(egreso)
        company_session.add(
            CfdiRelacionado(
                company_identifier=cid,
                uuid_origin=egreso.UUID,
                uuid_related=ingreso.UUID,
                TipoDeComprobante="E",
                TipoRelacion="01",
                is_issued=True,
                Estatus=True,
            )
        )

    company_session.flush()

    expected = sum(Decimal(str(tm)) for _, tm in montos)

    fields = ["UUID", "Total", "uuid_total_egresos_relacionados"]
    body = {
        "domain": [
            ["company_identifier", "=", cid],
            ["TipoDeComprobante", "=", "I"],
            ["UUID", "=", ingreso.UUID],
        ],
        "fuzzy_search": "",
        "fields": [f for f in fields if f not in SPECIAL_FIELDS],
        "limit": None,
        "offset": None,
        "order_by": None,
    }

    query = company_session.query()
    query = CFDIController.get_query(
        CFDIORM, fields=fields, body=body, aggregate=True, sql_query=query
    )
    query = CFDIController.apply_domain(
        query=query, domain=body["domain"], fuzzy_search="", session=company_session
    )

    row = query.first()
    assert row is not None
    total = Decimal(str(row._asdict()["Total egresos relacionados"]))
    assert total == expected
