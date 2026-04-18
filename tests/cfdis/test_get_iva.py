from datetime import date, datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from chalicelib.new.iva import IVAGetter
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import DoctoRelacionado, Payment
from chalicelib.schema.models.tenant.cfdi import CFDI


def test_get_iva(company_session: Session, company: Company):
    """Test de get_iva con múltiples escenarios: PPD acreditable, PPD excluido, PUE, y Nota de crédito"""

    # CFDI PPD con pago acreditable
    cfdi_ppd_acreditable = CFDI.demo(
        company_identifier=company.identifier,
        Fecha=datetime(2025, 9, 10, 10, 0, 0),
        FechaFiltro=datetime(2025, 9, 10, 10, 0, 0),
        Moneda="MXN",
        MetodoPago="PPD",
        TipoDeComprobante="I",
        ExcludeFromIVA=False,
        Estatus=True,
        is_issued=False,
        Version="4.0",
        Total=Decimal("2320.00"),
        SubTotalMXN=Decimal("2000.00"),
        BaseIVA16=Decimal("2000.00"),
        IVATrasladado16=Decimal("320.00"),
        TrasladosIVA=Decimal("320.00"),
        TrasladosIVAMXN=Decimal("320.00"),
        from_xml=True,
        Serie="A",
        Folio="001",
        RfcEmisor="XAXX010101000",
        NombreEmisor="PROVEEDOR TEST SA DE CV",
    )
    company_session.add(cfdi_ppd_acreditable)
    company_session.flush()

    payment_1 = Payment(
        company_identifier=company.identifier,
        FechaPago=datetime(2025, 10, 15, 10, 0, 0),
        FormaDePagoP="03",
        MonedaP="MXN",
        Monto=Decimal("2320.00"),
        uuid_origin="11111111-1111-1111-1111-111111111111",
        index=1,
        Estatus=True,
    )
    company_session.add(payment_1)
    company_session.flush()

    docto_relacionado_1 = DoctoRelacionado(
        company_identifier=company.identifier,
        payment_identifier=payment_1.identifier,
        UUID="11111111-1111-1111-1111-111111111111",
        UUID_related=cfdi_ppd_acreditable.UUID,
        FechaPago=datetime(2025, 10, 15, 10, 0, 0),
        MonedaDR="MXN",
        ExcludeFromIVA=False,
        ImpPagadoMXN=Decimal("2320.00"),
        BaseIVA16=Decimal("2000.00"),
        BaseIVA8=Decimal("0.00"),
        BaseIVA0=Decimal("0.00"),
        BaseIVAExento=Decimal("0.00"),
        IVATrasladado16=Decimal("320.00"),
        IVATrasladado8=Decimal("0.00"),
        TrasladosIVAMXN=Decimal("320.00"),
        RetencionesIVAMXN=Decimal("0.00"),
        Serie="A",
        Folio="001",
        Estatus=True,
    )
    company_session.add(docto_relacionado_1)
    company_session.flush()

    # CFDI PPD con pago excluido
    cfdi_ppd_excluido = CFDI.demo(
        company_identifier=company.identifier,
        Fecha=datetime(2025, 9, 15, 10, 0, 0),
        FechaFiltro=datetime(2025, 9, 15, 10, 0, 0),
        Moneda="MXN",
        MetodoPago="PPD",
        TipoDeComprobante="I",
        ExcludeFromIVA=False,
        Estatus=True,
        is_issued=False,
        Version="4.0",
        Total=Decimal("1160.00"),
        SubTotalMXN=Decimal("1000.00"),
        BaseIVA16=Decimal("1000.00"),
        IVATrasladado16=Decimal("160.00"),
        TrasladosIVA=Decimal("160.00"),
        TrasladosIVAMXN=Decimal("160.00"),
        from_xml=True,
        Serie="A",
        Folio="002",
        RfcEmisor="XAXX010101000",
        NombreEmisor="PROVEEDOR TEST SA DE CV",
    )
    company_session.add(cfdi_ppd_excluido)
    company_session.flush()

    payment_2 = Payment(
        company_identifier=company.identifier,
        FechaPago=datetime(2025, 10, 20, 10, 0, 0),
        FormaDePagoP="03",
        MonedaP="MXN",
        Monto=Decimal("1160.00"),
        uuid_origin="22222222-2222-2222-2222-222222222222",
        index=1,
        Estatus=True,
    )
    company_session.add(payment_2)
    company_session.flush()

    docto_relacionado_2 = DoctoRelacionado(
        company_identifier=company.identifier,
        payment_identifier=payment_2.identifier,
        UUID="22222222-2222-2222-2222-222222222222",
        UUID_related=cfdi_ppd_excluido.UUID,
        FechaPago=datetime(2025, 10, 20, 10, 0, 0),
        MonedaDR="MXN",
        ExcludeFromIVA=True,
        ImpPagadoMXN=Decimal("1160.00"),
        BaseIVA16=Decimal("1000.00"),
        BaseIVA8=Decimal("0.00"),
        BaseIVA0=Decimal("0.00"),
        BaseIVAExento=Decimal("0.00"),
        IVATrasladado16=Decimal("160.00"),
        IVATrasladado8=Decimal("0.00"),
        TrasladosIVAMXN=Decimal("160.00"),
        RetencionesIVAMXN=Decimal("0.00"),
        Serie="A",
        Folio="002",
        Estatus=True,
    )
    company_session.add(docto_relacionado_2)
    company_session.flush()

    # CFDI PUE acreditable
    cfdi_pue = CFDI.demo(
        company_identifier=company.identifier,
        Fecha=datetime(2025, 10, 10, 10, 0, 0),
        FechaFiltro=datetime(2025, 10, 10, 10, 0, 0),
        PaymentDate=datetime(2025, 10, 10, 10, 0, 0),
        Moneda="MXN",
        MetodoPago="PUE",
        TipoDeComprobante="I",
        ExcludeFromIVA=False,
        Estatus=True,
        is_issued=False,
        Version="4.0",
        Total=Decimal("580.00"),
        SubTotalMXN=Decimal("500.00"),
        BaseIVA16=Decimal("500.00"),
        IVATrasladado16=Decimal("80.00"),
        TrasladosIVA=Decimal("80.00"),
        TrasladosIVAMXN=Decimal("80.00"),
        Serie="B",
        Folio="100",
        RfcEmisor="XAXX010101000",
        NombreEmisor="PROVEEDOR TEST SA DE CV",
    )
    company_session.add(cfdi_pue)
    company_session.flush()

    # Nota de crédito
    cfdi_nota_credito = CFDI.demo(
        company_identifier=company.identifier,
        Fecha=datetime(2025, 10, 18, 10, 0, 0),
        FechaFiltro=datetime(2025, 10, 18, 10, 0, 0),
        PaymentDate=datetime(2025, 10, 18, 10, 0, 0),
        Moneda="MXN",
        MetodoPago="PUE",
        TipoDeComprobante="E",
        ExcludeFromIVA=False,
        Estatus=True,
        is_issued=False,
        Version="4.0",
        Total=Decimal("116.00"),
        SubTotalMXN=Decimal("100.00"),
        BaseIVA16=Decimal("100.00"),
        IVATrasladado16=Decimal("16.00"),
        TrasladosIVA=Decimal("16.00"),
        TrasladosIVAMXN=Decimal("16.00"),
        RetencionesIVAMXN=Decimal("0.00"),
        Serie="NC",
        Folio="001",
        RfcEmisor="XAXX010101000",
        NombreEmisor="PROVEEDOR TEST SA DE CV",
    )
    company_session.add(cfdi_nota_credito)
    company_session.flush()

    period = date(2025, 10, 1)
    getter = IVAGetter(company_session)
    result = getter.get_iva(period)

    # Asserts básicos
    assert result["period"]["creditable"]["total"] == Decimal("400.00")
    assert result["period"]["creditable"]["qty"] == 3
    assert result["period"]["creditable"]["excluded_qty"] == 1
