import random
import uuid
from datetime import date

from sqlalchemy.orm import Session

from chalicelib.new.isr_deducciones import calcular_totales_nomina_data
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.docto_relacionado import DoctoRelacionado
from chalicelib.schema.models.tenant.payment import Payment


def test_isr_totals_pre_llenado_pagos(company_session: Session, session: Session, company):
    """This test checks the ISR totals calculation for pre-filled payments.
    We check that the ISR totals are calculated correctly based on the ExcludeFromISR flag from DoctoRelacionado model."""

    cfdi_i = CFDI.demo(
        Fecha="2025-05-03T12:00:00", UsoCFDIReceptor="G03", company_identifier=company.identifier
    )

    cfdi_p = CFDI.demo(
        Fecha="2025-05-03T12:00:00",
        TipoDeComprobante="P",
        FormaDePago="01",
        company_identifier=company.identifier,
        Estatus=True,
        is_issued=True,
        ExcludeFromISR=False,  # For this scenario, this flag is not relevant for the calculation
    )

    payment = Payment(
        index=0,
        identifier=str(uuid.uuid4()),
        company_identifier=company.identifier,
        is_issued=True,
        uuid_origin=cfdi_p.UUID,
        FechaPago=date.fromisoformat("2025-05-08"),
        FormaDePagoP="01",
        MonedaP="MXN",
        Monto=random.random(),
    )

    payment_relation = DoctoRelacionado(
        company_identifier=company.identifier,
        payment_identifier=payment.identifier,
        FechaPago=date.fromisoformat("2025-05-08"),
        UUID=cfdi_p.UUID,
        UUID_related=cfdi_i.UUID,
        cfdi_origin=cfdi_p,
        cfdi_related=cfdi_i,
        MonedaDR="MXN",
        ExcludeFromISR=True,  # This flag indicates that this record should be excluded from ISR totals
        BaseIVA16=random.random(),
        BaseIVA8=random.random(),
        BaseIVA0=random.random(),
        BaseIVAExento=random.random(),
        IVATrasladado16=random.random(),
        IVATrasladado8=random.random(),
        TrasladosIVAMXN=random.random(),
        RetencionesIVAMXN=random.random(),
        RetencionesDR=[],
        TrasladosDR=[],
        is_issued=True,
    )

    company_session.add_all([cfdi_i, cfdi_p, payment, payment_relation])
    company_session.flush()

    domain = date.fromisoformat("2025-05-01")

    result = calcular_totales_nomina_data(
        company_session=company_session,
        session=session,
        company=company,
        domain=domain,
    )

    assert result["totals_table_excluded"][5]["ConteoCFDIs"] == 1
