from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi_excluded import ExcludedCFDIController
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import DoctoRelacionado, Payment
from chalicelib.schema.models.tenant.cfdi import CFDI


def test_excluded_cfdi_search_with_paid_by_filter(
    company_session: Session,
    company: Company,
):
    """Test que verifica búsqueda de CFDIs excluidos con filtro de paid_by para facturas PPD"""

    # Crear CFDI de tipo ingreso PPD (pago diferido)
    cfdi_ppd = CFDI.demo(
        company_identifier=company.identifier,
        Fecha=datetime(2025, 9, 15, 10, 0, 0),  # Fecha de emisión en septiembre
        FechaFiltro=datetime(2025, 9, 15, 10, 0, 0),
        Moneda="MXN",
        MetodoPago="PPD",
        TipoDeComprobante="I",
        ExcludeFromIVA=False,  # No excluido en el CFDI original
        Estatus=True,
        is_issued=False,
        Version="4.0",
        Total=Decimal("1160.00"),
        SubTotalMXN=Decimal("1000.00"),
        BaseIVA16=Decimal("1000.00"),
        IVATrasladado16=Decimal("160.00"),
        TrasladosIVA=Decimal("160.00"),
        Serie="A",
        Folio="001",
        RfcEmisor="XAXX010101000",
        NombreEmisor="PROVEEDOR TEST SA DE CV",
    )
    company_session.add(cfdi_ppd)
    company_session.flush()

    # Crear CFDI de pago (complemento de pago)
    payment_cfdi_uuid = "12345678-1234-1234-1234-123456789012"

    # Crear registro de pago
    payment = Payment(
        company_identifier=company.identifier,
        FechaPago=datetime(2025, 10, 20, 10, 0, 0),  # Fecha de pago en octubre
        FormaDePagoP="03",  # Transferencia
        MonedaP="MXN",
        Monto=Decimal("1160.00"),
        uuid_origin=payment_cfdi_uuid,  # UUID del CFDI de pago
        index=1,
        Estatus=True,
    )
    company_session.add(payment)
    company_session.flush()

    # Crear relación de pago (DoctoRelacionado)
    # Este registro vincula el pago con la factura original
    docto_relacionado = DoctoRelacionado(
        company_identifier=company.identifier,
        payment_identifier=payment.identifier,
        UUID=payment_cfdi_uuid,  # UUID del CFDI de pago
        UUID_related=cfdi_ppd.UUID,  # UUID de la factura que se está pagando
        FechaPago=datetime(2025, 10, 20, 10, 0, 0),
        MonedaDR="MXN",
        ExcludeFromIVA=True,  # Este pago SÍ está excluido
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
        Folio="001",
        Estatus=True,
    )
    company_session.add(docto_relacionado)
    company_session.flush()

    # Domain: buscar CFDIs cuyos pagos estén en octubre y estén excluidos
    domain = [
        ["Estatus", "=", True],
        ["is_issued", "=", False],
        ["TipoDeComprobante", "in", ["I", "E"]],
        [
            "|",
            [
                # Opción 1: Facturas PPD con pagos en octubre excluidos
                [
                    ["PaymentDate", ">=", "2025-10-01T00:00:00.000"],
                    ["PaymentDate", "<", "2025-11-01T00:00:00.000"],
                ],
                # Opción 2: Facturas PUE emitidas en octubre excluidas
                [
                    ["Fecha", ">=", "2025-10-01T00:00:00.000"],
                    ["Fecha", "<", "2025-11-01T00:00:00.000"],
                    ["MetodoPago", "=", "PUE"],
                    ["Version", "=", "4.0"],
                ],
            ],
        ],
        ["ExcludeFromIVA", "=", True],
    ]

    # Llamar al método search
    resultado, next_page, total = ExcludedCFDIController.search(
        domain=domain,
        fields=[],
        order_by="",
        fuzzy_search="",
        limit=30,
        offset=0,
        session=company_session,
    )

    # Verificaciones
    assert len(resultado) > 0, "Debería encontrar la factura PPD con pago excluido en octubre"
    assert total == 1, f"Total debería ser 1, pero fue {total}"

    # Verificar que el resultado es el CFDI correcto
    result_row = resultado[0]
    assert result_row.UUID == cfdi_ppd.UUID, "Debería retornar el CFDI PPD"
    # El PaymentDate debería venir del DoctoRelacionado, no del CFDI
    assert result_row.PaymentDate == datetime(2025, 10, 20, 10, 0, 0), (
        "PaymentDate debería ser la fecha del pago, no del CFDI"
    )


def test_pushdown_filters_generate_where_inside_union(
    company_session: Session,
    company: Company,
):
    """Verifica que los filtros pushdown generan WHERE dentro de cada parte del UNION ALL.

    Sin pushdown el UNION procesa TODOS los registros y filtra después.
    Con pushdown cada SELECT filtra ANTES de unir, reduciendo filas procesadas.
    """

    query_sin = ExcludedCFDIController._get_query_model(session=company_session, fields=[])
    sql_sin = str(query_sin)
    parts_sin = sql_sin.split("UNION ALL")
    assert len(parts_sin) == 2
    assert "WHERE" not in parts_sin[1], "Sin pushdown no debe haber WHERE en DoctoRelacionado"

    domain = [
        ["Estatus", "=", True],
        ["is_issued", "=", False],
        ["ExcludeFromIVA", "=", True],
    ]
    query_con = ExcludedCFDIController._get_query_model(
        session=company_session, fields=[], domain=domain
    )

    sql_con = str(query_con)
    parts_con = sql_con.split("UNION ALL")
    assert len(parts_con) == 2
    assert "WHERE" in parts_con[0], "Con pushdown: CFDI debe tener WHERE"
    assert "WHERE" in parts_con[1], "Con pushdown: DoctoRelacionado debe tener WHERE"
