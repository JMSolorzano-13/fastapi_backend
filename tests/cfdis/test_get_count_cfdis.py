import random
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.new.shared.domain.primitives import identifier_default_factory
from chalicelib.schema.models import CfdiRelacionado
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import DoctoRelacionado, Payment
from chalicelib.schema.models.tenant.cfdi import CFDI


def test_get_count_cfdis_basic(company_session: Session, company: Company):
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

    company_schema = company.identifier
    domain = [
        ["company_identifier", "=", f"{company_schema}"],
        ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
        ["FechaFiltro", "<", "2025-02-01T00:00:00.000"],
        ["is_issued", "=", False],
        ["Estatus", "=", True],
        ["MetodoPago", "=", "PPD"],
        ["balance", ">", 0],
    ]

    fuzzy_search = ""

    CFDIController.count_cfdis_by_type(
        domain,
        fuzzy_search,
        session=company_session,
    )
