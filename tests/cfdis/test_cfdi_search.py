"""
Pruebas integrales de búsqueda de CFDI cubriendo distintos escenarios de filtrado.

Cubren:
- Búsqueda básica con filtro de saldo (balance)
- Filtro por MetodoPago (PPD, PUE)
- Filtro por TipoDeComprobante (I, E, P)
- Filtros por rango de fechas
- Recuperación de múltiples campos
- Emitidos vs Recibidos (bandera is_issued)
- Acceso a campos anidados (relaciones complejas)
"""

import json
import random
from datetime import datetime
from decimal import Decimal

from chalice.test import Client
from sqlalchemy.orm import Query, Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.new.shared.domain.primitives import identifier_default_factory
from chalicelib.schema.models import CfdiRelacionado
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import DoctoRelacionado, Payment
from chalicelib.schema.models.tenant.cfdi import CFDI


# CAMPOS CALCULADOS
def test_cfdi_search_basic_with_balance(
    client_authenticated: Client,
    company_session: Session,
    company: Company,
):
    """Prueba básica de búsqueda con filtro de saldo (balance)."""
    # Crear CFDIs con distintos saldos
    cfdi_with_balance = CFDI.demo(
        company_identifier=company.identifier,
        Total=Decimal("1000.00"),
        is_issued=True,
        Estatus=True,
    )
    cfdi_without_balance = CFDI.demo(
        company_identifier=company.identifier,
        Total=Decimal("0.00"),
        is_issued=True,
        Estatus=True,
    )

    company_session.add_all([cfdi_with_balance, cfdi_without_balance])
    company_session.commit()

    # Buscar CFDIs con balance > 0
    result = client_authenticated.http.post(
        "/CFDI/search",
        body=json.dumps(
            {
                "domain": [
                    ["company_identifier", "=", company.identifier],
                    ["balance", ">", 0],
                ],
                "fields": ["UUID", "Total", "balance"],
                "fuzzy_search": "",
                "limit": 10,
                "offset": 0,
            }
        ),
    )

    assert result.status_code == 200
    assert "data" in result.json_body
    assert len(result.json_body["data"]) == 1
    assert result.json_body["data"][0]["UUID"] == cfdi_with_balance.UUID


# COMPARACIONES CON VARIOS FILTROS PPD PUE E I P
def test_cfdi_search_filter(
    company_session: Session,
    company: Company,
):
    """Prueba de búsqueda con múltiples combinaciones de filtros."""
    # Crear CFDIs con distintas características
    cfdi_issued_ppd_ingreso = CFDI.demo(
        company_identifier=company.identifier,
        FechaFiltro=datetime(2023, 2, 15),
        Fecha=datetime(2023, 2, 15),
        is_issued=True,
        Estatus=True,
        MetodoPago="PPD",
        TipoDeComprobante="I",
        Total=Decimal("1000.00"),
    )
    cfdi_issued_pue_egreso = CFDI.demo(
        company_identifier=company.identifier,
        FechaFiltro=datetime(2023, 3, 20),
        Fecha=datetime(2023, 3, 20),
        is_issued=True,
        Estatus=True,
        MetodoPago="PUE",
        TipoDeComprobante="E",
        Total=Decimal("500.00"),
    )
    cfdi_received_ppd_ingreso = CFDI.demo(
        company_identifier=company.identifier,
        FechaFiltro=datetime(2023, 4, 10),
        Fecha=datetime(2023, 4, 10),
        is_issued=False,
        Estatus=True,
        MetodoPago="PPD",
        TipoDeComprobante="I",
        Total=Decimal("2000.00"),
    )
    cfdi_received_pue_ingreso = CFDI.demo(
        company_identifier=company.identifier,
        FechaFiltro=datetime(2023, 5, 5),
        Fecha=datetime(2023, 5, 5),
        is_issued=False,
        Estatus=True,
        MetodoPago="PUE",
        TipoDeComprobante="I",
        Total=Decimal("1500.00"),
    )
    cfdi_issued_pago = CFDI.demo(
        company_identifier=company.identifier,
        FechaFiltro=datetime(2023, 6, 1),
        Fecha=datetime(2023, 6, 1),
        is_issued=True,
        Estatus=True,
        TipoDeComprobante="P",
        Total=Decimal("0.00"),
    )

    company_session.add_all(
        [
            cfdi_issued_ppd_ingreso,
            cfdi_issued_pue_egreso,
            cfdi_received_ppd_ingreso,
            cfdi_received_pue_ingreso,
            cfdi_issued_pago,
        ]
    )
    company_session.commit()

    # Búsqueda 1: is_issued=True Y MetodoPago=PPD
    domain1 = [
        ["company_identifier", "=", company.identifier],
        ["is_issued", "=", True],
        ["MetodoPago", "=", "PPD"],
    ]
    records1, _, total1 = CFDIController.search(
        domain=domain1,
        fields=["UUID", "is_issued", "MetodoPago", "TipoDeComprobante"],
        order_by='"Fecha" asc',
        fuzzy_search="",
        limit=30,
        offset=0,
        session=company_session,
    )
    results1 = [dict(r._mapping) for r in records1]
    assert total1 == 1
    assert len(results1) == 1
    assert results1[0]["UUID"] == cfdi_issued_ppd_ingreso.UUID
    assert results1[0]["MetodoPago"] == "PPD"
    assert results1[0]["TipoDeComprobante"] == "I"

    # Búsqueda 2: is_issued=False Y TipoDeComprobante=I
    domain2 = [
        ["company_identifier", "=", company.identifier],
        ["is_issued", "=", False],
        ["TipoDeComprobante", "=", "I"],
    ]
    records2, _, total2 = CFDIController.search(
        domain=domain2,
        fields=["UUID", "is_issued", "MetodoPago", "TipoDeComprobante", "Total"],
        order_by='"Fecha" asc',
        fuzzy_search="",
        limit=30,
        offset=0,
        session=company_session,
    )
    results2 = [dict(r._mapping) for r in records2]
    assert total2 == 2
    assert len(results2) == 2
    assert results2[0]["UUID"] == cfdi_received_ppd_ingreso.UUID
    assert results2[1]["UUID"] == cfdi_received_pue_ingreso.UUID
    assert all(r["is_issued"] is False for r in results2)
    assert all(r["TipoDeComprobante"] == "I" for r in results2)

    # Búsqueda 3: MetodoPago=PUE Y TipoDeComprobante=E
    domain3 = [
        ["company_identifier", "=", company.identifier],
        ["MetodoPago", "=", "PUE"],
        ["TipoDeComprobante", "=", "E"],
    ]
    records3, _, total3 = CFDIController.search(
        domain=domain3,
        fields=["UUID", "is_issued", "MetodoPago", "TipoDeComprobante"],
        order_by='"Fecha" asc',
        fuzzy_search="",
        limit=30,
        offset=0,
        session=company_session,
    )
    results3 = [dict(r._mapping) for r in records3]
    assert total3 == 1
    assert len(results3) == 1
    assert results3[0]["UUID"] == cfdi_issued_pue_egreso.UUID
    assert results3[0]["MetodoPago"] == "PUE"
    assert results3[0]["TipoDeComprobante"] == "E"


# BÚSQUEDA CON CAMPOS TO-MANY Y TO-ONE
def test_cfdi_search_complex_fields(
    company_session: Session,
    company: Company,
):
    """Prueba de búsqueda recuperando campos complejos/anidados (similar a producción)."""
    # Crear un CFDI con campos relevantes
    cfdi = CFDI.demo(
        company_identifier=company.identifier,
        FechaFiltro=datetime(2023, 6, 15),
        Fecha=datetime(2023, 6, 15),
        MetodoPago="PUE",
        TipoDeComprobante="I",
        is_issued=False,
        Estatus=True,
        Total=Decimal("11600.00"),
        SubTotal=Decimal("10000.00"),
        TrasladosIVA=Decimal("1600.00"),
        Moneda="MXN",
        TipoCambio=Decimal("1.00"),
        FormaPago="01",
        UsoCFDIReceptor="G03",
        Serie="A",
        Folio="12345",
        RfcEmisor="XAXX010101000",
        NombreEmisor="PROVEEDOR TEST SA DE CV",
        RfcReceptor="XEXX010101000",
        NombreReceptor="EMPRESA TEST SA DE CV",
        from_xml=True,
        ExcludeFromIVA=False,
    )

    company_session.add(cfdi)
    company_session.flush()

    # Agregar una relación de pago para que el campo to-many no esté vacío
    docto = DoctoRelacionado.demo(
        company_identifier=company.identifier,
        UUID_related=cfdi.UUID,
    )
    company_session.add(docto)
    company_session.commit()

    domain = [
        ["company_identifier", "=", company.identifier],
        ["FechaFiltro", ">=", "2023-01-01T00:00:00.000"],
        ["FechaFiltro", "<", "2024-01-01T00:00:00.000"],
        ["Estatus", "=", True],
        ["MetodoPago", "=", "PUE"],
        ["is_issued", "=", False],
        ["TipoDeComprobante", "=", "I"],
    ]
    fields = [
        "UUID",
        "from_xml",
        "TipoDeComprobante",
        "MetodoPago",
        "ExcludeFromIVA",
        # relación a muchos (to-many)
        "paid_by.UUID",
        # relación uno a uno (to-one, catálogo)
        "c_forma_pago.name",
    ]

    records, next_page, total = CFDIController.search(
        domain=domain,
        fields=fields,
        order_by='"Fecha" asc, "UUID" asc',
        fuzzy_search="",
        limit=30,
        offset=0,
        session=company_session,
    )

    assert total == 1
    assert len(records) == 1

    data = dict(records[0]._mapping)
    assert data["UUID"] == cfdi.UUID
    assert data["from_xml"] is True
    assert data["TipoDeComprobante"] == "I"
    assert data["MetodoPago"] == "PUE"
    assert data["ExcludeFromIVA"] is False

    # Validar que el agregado to-many regrese elementos
    assert "paid_by" in data
    paid_by_list = data["paid_by"]
    # Con filas relacionadas se espera estructura tipo lista
    if isinstance(paid_by_list, str):
        # Algunos drivers devuelven JSONB como cadena si está vacío; aquí no debe estar vacío
        assert paid_by_list != "[]"
    else:
        assert isinstance(paid_by_list, list)
        assert len(paid_by_list) >= 1

    # Validar presencia de campo de catálogo to-one (el valor puede ser None)
    assert "c_forma_pago.name" in data


def test_cfdi_search_pagination(
    company_session: Session,
    company: Company,
):
    """Prueba de búsqueda con paginación (limit y offset)."""
    # Crear múltiples CFDIs
    cfdis = [
        CFDI.demo(
            company_identifier=company.identifier,
            FechaFiltro=datetime(2023, 1, i + 1),
            is_issued=True,
            Estatus=True,
        )
        for i in range(5)
    ]

    company_session.add_all(cfdis)
    company_session.commit()

    # Primera página (limit 2)
    domain = [["company_identifier", "=", company.identifier]]
    fields = ["UUID"]
    order_by = '"FechaFiltro" asc'

    records_page1, next1, total1 = CFDIController.search(
        domain=domain,
        fields=fields,
        order_by=order_by,
        limit=2,
        offset=0,
        fuzzy_search="",
        session=company_session,
    )

    # Segunda página (offset 2)
    records_page2, next2, total2 = CFDIController.search(
        domain=domain,
        fields=fields,
        order_by=order_by,
        limit=2,
        offset=2,
        fuzzy_search="",
        session=company_session,
    )

    results_page1 = [dict(r._mapping) for r in records_page1]
    results_page2 = [dict(r._mapping) for r in records_page2]

    assert len(results_page1) == 2
    assert len(results_page2) >= 1  # Al menos 1 resultado en la página 2
    # Verificar que no haya traslape entre páginas
    page1_uuids = {item["UUID"] for item in results_page1}
    page2_uuids = {item["UUID"] for item in results_page2}
    assert len(page1_uuids.intersection(page2_uuids)) == 0
    # Verificar que el total sea correcto
    assert len(page1_uuids) + len(page2_uuids) <= 5


# PRUEBA CON COMBINACIÓN DE CAMPOS QUE PUEDE PROVOCAR PRODUCTO CARTESIANO
def test_cfdi_search_producto_cartesiano(
    company_session: Session,
    session: Session,
    company: Company,
):
    """
    Prueba integral de búsqueda de CFDI con relaciones anidadas complejas.

    Cubre:
    - 40+ CFDIs de ingreso (tipo I) con MetodoPago PPD
    - CFDIs de pago (tipo P) con múltiples pagos
    - Un ingreso con 5+ pagos
    - CFDIs de egreso (tipo E) relacionados a ingresos
    - Acceso a campos a través de relaciones anidadas:
      * paid_by.UUID
      * paid_by.cfdi_related.Estatus
      * cfdi_related.uuid_origin
      * active_egresos.Total
      * pays.ImpPagado
    """

    random.seed(42)

    # ========================================================================
    # 1. Crear 45 CFDIs de Ingreso (tipo I) con MetodoPago PPD
    # ========================================================================
    cfdis_ingreso = []
    for i in range(45):
        cfdi = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=datetime(2025, 1, 2 + (i % 28)),  # Distribuidos a lo largo de enero
            FechaFiltro=datetime(2025, 1, 2 + (i % 28)),
            PaymentDate=datetime(2025, 1, 2 + (i % 28)),
            Moneda="MXN",
            MetodoPago="PPD",  # Pago en parcialidades
            TipoDeComprobante="I",
            ExcludeFromISR=False,
            ExcludeFromIVA=False,
            Estatus=True,
            is_issued=False,
            UsoCFDIReceptor=random.choice(["G01", "G03"]),
            Total=Decimal(str(random.randint(1000, 10000))),
            SubTotalMXN=Decimal(str(random.randint(800, 8000))),
            DescuentoMXN=Decimal(str(random.randint(0, 100))),
            TrasladosIVAMXN=Decimal(str(random.randint(100, 1600))),
            Serie=f"A{i:03d}",
            Folio=f"{1000 + i}",
            RfcEmisor="XAXX010101000",
            NombreEmisor=f"PROVEEDOR {i} SA DE CV",
        )
        cfdis_ingreso.append(cfdi)

    company_session.add_all(cfdis_ingreso)
    company_session.flush()  # Obtener UUIDs

    # ========================================================================
    # 2. Crear CFDIs de Egreso (tipo E) relacionados a algunos ingresos
    # ========================================================================
    cfdis_egreso = []
    cfdi_relacionados_egreso = []

    # Crear 10 egresos relacionados a los primeros 10 ingresos
    for i in range(10):
        egreso = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=datetime(2025, 1, 15 + i),
            FechaFiltro=datetime(2025, 1, 15 + i),
            PaymentDate=datetime(2025, 1, 15 + i),
            Moneda="MXN",
            TipoDeComprobante="E",  # Egreso (nota de crédito)
            ExcludeFromISR=False,
            Estatus=True,
            is_issued=False,
            Total=Decimal(str(random.randint(100, 500))),
            SubTotalMXN=Decimal(str(random.randint(80, 400))),
            Serie=f"E{i:03d}",
            Folio=f"{2000 + i}",
        )
        cfdis_egreso.append(egreso)

        # Crear relación entre egreso e ingreso
        cfdi_rel = CfdiRelacionado(
            company_identifier=company.identifier,
            uuid_origin=egreso.UUID,
            uuid_related=cfdis_ingreso[i].UUID,
            TipoDeComprobante="E",
            is_issued=False,
            Estatus=True,
            TipoRelacion="01",
        )
        cfdi_relacionados_egreso.append(cfdi_rel)

    company_session.add_all(cfdis_egreso)
    company_session.add_all(cfdi_relacionados_egreso)
    company_session.flush()

    cfdis_pago = []
    payments = []
    doctos_relacionados = []

    ingreso_with_5_payments = cfdis_ingreso[0]

    for payment_idx in range(5):
        # Crear CFDI de Pago
        cfdi_pago = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=datetime(2025, 1, 10 + payment_idx),
            FechaFiltro=datetime(2025, 1, 10 + payment_idx),
            PaymentDate=datetime(2025, 1, 10 + payment_idx),
            Moneda="MXN",
            TipoDeComprobante="P",
            ExcludeFromISR=False,
            Estatus=True,
            is_issued=False,
            Total=Decimal("0"),
            SubTotalMXN=Decimal("0"),
            Serie=f"P{payment_idx:03d}",
            Folio=f"{3000 + payment_idx}",
        )
        cfdis_pago.append(cfdi_pago)

        payment_id = identifier_default_factory()
        payment = Payment(
            identifier=payment_id,
            company_identifier=company.identifier,
            is_issued=False,
            uuid_origin=cfdi_pago.UUID,
            index=0,
            FechaPago=datetime(2025, 1, 10 + payment_idx),
            FormaDePagoP=random.choice(["02", "03", "04", "28"]),
            MonedaP="MXN",
            Monto=Decimal(str(random.randint(500, 2000))),
            TipoCambioP=Decimal("1.0"),
            Estatus=True,
        )
        payments.append(payment)

        docto = DoctoRelacionado.demo(
            company_identifier=company.identifier,
            is_issued=False,
            payment_identifier=payment_id,
            UUID=cfdi_pago.UUID,
            UUID_related=ingreso_with_5_payments.UUID,
            FechaPago=datetime(2025, 1, 10 + payment_idx),
            MonedaDR="MXN",
            EquivalenciaDR=Decimal("1.0"),
            NumParcialidad=payment_idx + 1,
            ImpPagado=Decimal(str(random.randint(500, 2000))),
            ImpPagadoMXN=Decimal(str(random.randint(500, 2000))),
            active=True,
            Estatus=True,
        )
        doctos_relacionados.append(docto)

    payment_counter = 5
    for ingreso_idx in range(1, 25):
        ingreso = cfdis_ingreso[ingreso_idx]
        num_payments = random.randint(1, 3)

        for payment_idx in range(num_payments):
            # Crear CFDI de Pago
            cfdi_pago = CFDI.demo(
                company_identifier=company.identifier,
                Fecha=datetime(2025, 1, 10 + payment_counter % 20),
                FechaFiltro=datetime(2025, 1, 10 + payment_counter % 20),
                PaymentDate=datetime(2025, 1, 10 + payment_counter % 20),
                Moneda="MXN",
                TipoDeComprobante="P",
                ExcludeFromISR=False,
                Estatus=True,
                is_issued=False,
                Total=Decimal("0"),
                SubTotalMXN=Decimal("0"),
                Serie=f"P{payment_counter:03d}",
                Folio=f"{3000 + payment_counter}",
            )
            cfdis_pago.append(cfdi_pago)

            payment_id = identifier_default_factory()
            payment = Payment(
                identifier=payment_id,
                company_identifier=company.identifier,
                is_issued=False,
                uuid_origin=cfdi_pago.UUID,
                index=0,
                FechaPago=datetime(2025, 1, 10 + payment_counter % 20),
                FormaDePagoP=random.choice(["02", "03", "04", "28"]),
                MonedaP="MXN",
                Monto=Decimal(str(random.randint(500, 2000))),
                TipoCambioP=Decimal("1.0"),
                Estatus=True,
            )
            payments.append(payment)

            docto = DoctoRelacionado.demo(
                company_identifier=company.identifier,
                is_issued=False,
                payment_identifier=payment_id,
                UUID=cfdi_pago.UUID,
                UUID_related=ingreso.UUID,
                FechaPago=datetime(2025, 1, 10 + payment_counter % 20),
                MonedaDR="MXN",
                EquivalenciaDR=Decimal("1.0"),
                NumParcialidad=payment_idx + 1,
                ImpPagado=Decimal(str(random.randint(500, 2000))),
                ImpPagadoMXN=Decimal(str(random.randint(500, 2000))),
                active=True,
                Estatus=True,
            )
            doctos_relacionados.append(docto)
            payment_counter += 1

    company_session.add_all(cfdis_pago)
    company_session.add_all(payments)
    company_session.add_all(doctos_relacionados)
    company_session.commit()

    # Llamar directamente al método search
    domain = [
        ["company_identifier", "=", company.identifier],
        ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
        ["FechaFiltro", "<", "2025-02-01T00:00:00.000"],
        ["MetodoPago", "=", "PPD"],
        # ["balance", "<=", 0],
        ["is_issued", "=", False],
        ["TipoDeComprobante", "=", "I"],
    ]

    # Campos mínimos para ejercitar relaciones to-many y to-one
    fields = [
        "UUID",
        "MetodoPago",
        # to-many principal (pagos aplicados al ingreso)
        "paid_by.UUID",
        # nested to-many dentro de to-many para detectar producto cartesiano
        "paid_by.cfdi_related.Estatus",
        # to-many relacionado (pays para comparar cardinalidades)
        "pays.ImpPagado",
        "pays.ImpPagadoMXN",
        # to-many egresos/relacionados
        "cfdi_related.uuid_origin",
        "cfdi_related.TipoDeComprobante",
        # to-many egresos activos con totales
        "active_egresos.Total",
    ]

    records, next_page, total_records = CFDIController.search(
        domain=domain,
        fields=fields,
        order_by='"Fecha" asc , "UUID" asc',
        fuzzy_search="",
        limit=30,
        offset=0,
        session=company_session,
    )
    query = CFDIController._search(
        domain=domain,
        fields=fields,
        order_by='"Fecha" asc , "UUID" asc',
        fuzzy_search="",
        limit=30,
        offset=0,
        session=company_session,
        lazzy=True,
    )
    assert isinstance(query, Query)
    print(str(query))

    records_list = [dict(record._mapping) for record in records]

    # Totales: sin productos cartesianos a nivel de filas
    assert total_records == 45
    assert len(records_list) == 30

    # Identificar el ingreso con 5 pagos por UUID conocido
    ingreso_uuid = cfdis_ingreso[0].UUID
    r0 = next((r for r in records_list if r.get("UUID") == ingreso_uuid), None)
    assert r0 is not None

    # Sin producto cartesiano entre paid_by y paid_by.cfdi_related
    assert r0.get("MetodoPago") == "PPD"
    assert len(r0["paid_by"]) == 5
    assert len(r0["pays"]) == 0
    # Validar unicidad por UUID dentro de paid_by
    paid_by_uuids = {p.get("UUID") for p in r0.get("paid_by", [])}
    assert len(paid_by_uuids) == 5

    # Egreso relacionado presente y activo
    rel = r0.get("cfdi_related", [])
    assert len(rel) >= 1
    assert rel[0].get("TipoDeComprobante") == "E"

    # Egresos activos con total > 0
    act = r0.get("active_egresos", [])
    assert len(act) >= 1
    assert float(act[0]["Total"]) > 0


def test_search_no_fields(company_session: Session):
    company_session.add(CFDI.demo())
    res = CFDIController._search(domain=[], fields=[], session=company_session, need_count=True)
    assert res[1] == 1, "Must contains the only CFDI created"
