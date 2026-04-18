import random
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.controllers.enums import ResumeType
from chalicelib.new.shared.domain.primitives import identifier_default_factory
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import DoctoRelacionado, Payment
from chalicelib.schema.models.tenant.cfdi import CFDI


def test_resume_basico_con_filtro_PPD_balance(
    company_session: Session,
    company: Company,
):
    cfdis_ingreso = []
    # Crear algunos CFDIs con MetodoPago PUE (balance = 0 automáticamente)
    for i in range(15):
        total = Decimal(str(1000 + (i * 100)))  # Valores diferentes para cada CFDI
        subtotal = (total / Decimal("1.16")).quantize(Decimal("0.01"))
        iva = total - subtotal

        cfdi = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=datetime(2025, 1, 1, 10, i, 0),  # Diferentes minutos
            FechaFiltro=datetime(2025, 1, 1, 10, i, 0),
            PaymentDate=datetime(2025, 1, 1, 10, i, 0),
            Moneda="MXN",
            MetodoPago="PUE",  # Pago en una sola exhibición (balance = 0)
            TipoDeComprobante="I",
            ExcludeFromISR=False,
            ExcludeFromIVA=False,
            Estatus=True,
            is_issued=False,
            UsoCFDIReceptor="G01",
            Total=total,
            SubTotalMXN=subtotal,
            DescuentoMXN=Decimal("0.00"),
            TrasladosIVAMXN=iva,
            NetoMXN=subtotal,
            Serie="PUE",
            Folio=f"{1000 + i}",
            RfcEmisor="XAXX010101000",
            NombreEmisor=f"PROVEEDOR {i} SA DE CV",
        )
        cfdis_ingreso.append(cfdi)

    # Crear un único CFDI con MetodoPago PPD y balance > 0
    cfdi_ppd = CFDI.demo(
        company_identifier=company.identifier,
        Fecha=datetime(2025, 1, 1, 11, 0, 0),
        FechaFiltro=datetime(2025, 1, 1, 11, 0, 0),
        PaymentDate=datetime(2025, 1, 1, 11, 0, 0),
        Moneda="MXN",
        MetodoPago="PPD",  # Pago en parcialidades (tiene balance > 0)
        TipoDeComprobante="I",
        ExcludeFromISR=False,
        ExcludeFromIVA=False,
        Estatus=True,
        is_issued=False,
        UsoCFDIReceptor="G03",
        Total=Decimal("5000.00"),
        SubTotalMXN=Decimal("4310.34"),
        DescuentoMXN=Decimal("0.00"),
        TrasladosIVAMXN=Decimal("689.66"),
        NetoMXN=Decimal("4310.34"),
        Serie="PPD",
        Folio="2001",
        RfcEmisor="XAXX010101000",
        NombreEmisor="PROVEEDOR PPD SA DE CV",
    )
    cfdis_ingreso.append(cfdi_ppd)

    company_session.add_all(cfdis_ingreso)
    company_session.flush()

    domain = [
        ["company_identifier", "=", company.identifier],
        ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
        ["FechaFiltro", "<", "2025-01-02T00:00:00.000"],
        ["Estatus", "=", True],
        ["MetodoPago", "=", "PPD"],
        ["balance", ">", 0],
        ["is_issued", "=", False],
        ["TipoDeComprobante", "=", "I"],
    ]
    fuzzy_search = ""
    resume_type = ResumeType.BASIC.name

    resultado = CFDIController.resume(
        domain,
        fuzzy_search,
        resume_type=resume_type,
        session=company_session,
    )

    # Verificar que el filtro de balance funciona correctamente
    assert resultado["filtered"]["count"] == 1
    assert resultado["filtered"]["Total"] == Decimal("5000.00")


def test_resume_payroll(company_session: Session, company: Company):
    from chalicelib.schema.models.tenant.nomina import Nomina

    cfdis_nomina = []
    nominas = []

    # Crear 5 CFDIs de nómina con sus respectivos registros de Nomina
    for i in range(5):
        cfdi = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=datetime(2025, 1, 1, 10, i, 0),
            FechaFiltro=datetime(2025, 1, 1, 10, i, 0),
            PaymentDate=datetime(2025, 1, 1, 10, i, 0),
            Moneda="MXN",
            MetodoPago="PUE",
            TipoDeComprobante="N",
            ExcludeFromISR=False,
            ExcludeFromIVA=False,
            Estatus=True,
            is_issued=True,  # Las nóminas se emiten
            UsoCFDIReceptor="CN01",  # Uso para nómina
            Total=Decimal("5000.00"),
            SubTotalMXN=Decimal("5000.00"),
            DescuentoMXN=Decimal("0.00"),
            TrasladosIVAMXN=Decimal("0.00"),
            NetoMXN=Decimal("5000.00"),
            Serie="N",
            Folio=f"{1000 + i}",
            RfcEmisor=company.rfc,
            NombreEmisor=company.name,
            RfcReceptor=f"EMPL{i:06d}",
            NombreReceptor=f"EMPLEADO {i}",
        )
        cfdis_nomina.append(cfdi)

        # Crear registro de Nomina asociado
        nomina = Nomina(
            company_identifier=company.identifier,
            cfdi_uuid=cfdi.UUID,
            # Campos obligatorios
            Version="1.2",
            TipoNomina="O",  # Nómina ordinaria
            FechaPago=datetime(2025, 1, 1, 10, i, 0),
            FechaInicialPago=datetime(2024, 12, 16),
            FechaFinalPago=datetime(2024, 12, 31),
            NumDiasPagados=Decimal("15"),
            # Receptor - campos obligatorios
            ReceptorCurp=f"CURP{i:012d}ABC",
            ReceptorTipoContrato="01",  # Por tiempo indeterminado
            ReceptorTipoRegimen="02",  # Sueldos
            ReceptorNumEmpleado=f"EMP{i:05d}",
            ReceptorPeriodicidadPago="04",  # Quincenal
            ReceptorClaveEntFed="DIF",  # Ciudad de México
            # Montos
            TotalPercepciones=Decimal("6000.00"),
            TotalDeducciones=Decimal("1000.00"),
            TotalOtrosPagos=Decimal("0.00"),
            PercepcionesTotalSueldos=Decimal("5000.00"),
            PercepcionesTotalGravado=Decimal("5000.00"),
            PercepcionesTotalExento=Decimal("1000.00"),
            DeduccionesTotalImpuestosRetenidos=Decimal("800.00"),
            DeduccionesTotalOtrasDeducciones=Decimal("200.00"),
            SubsidioCausado=Decimal("0.00"),
            AjusteISRRetenido=Decimal("0.00"),
            PercepcionesJubilacionPensionRetiro=Decimal("0.00"),
            PercepcionesSeparacionIndemnizacion=Decimal("0.00"),
        )
        nominas.append(nomina)

    company_session.add_all(cfdis_nomina)
    company_session.add_all(nominas)
    company_session.flush()

    # Domain simplificado para nóminas (sin balance ni MetodoPago específico)
    domain = [
        ["company_identifier", "=", company.identifier],
        ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
        ["FechaFiltro", "<", "2025-01-02T00:00:00.000"],
        ["Estatus", "=", True],
        ["is_issued", "=", True],
        ["TipoDeComprobante", "=", "N"],
    ]
    fuzzy_search = ""
    resume_type = ResumeType.N.name

    resultado = CFDIController.resume(
        domain,
        fuzzy_search,
        resume_type=resume_type,
        session=company_session,
    )

    # Verificaciones
    assert resultado["filtered"]["Qty"] == 5
    assert resultado["filtered"]["EmpleadosQty"] == 5
    assert resultado["filtered"]["TotalPercepciones"] == Decimal("30000.00")
    assert resultado["filtered"]["TotalDeducciones"] == Decimal("5000.00")
    assert resultado["filtered"]["NetoAPagar"] == Decimal("25000.00")


def test_resume_issued_advance_payment_filter(company_session: Session, company: Company):
    random.seed(42)

    # ========================================================================
    # 1. Crear 45 CFDIs de Ingreso (tipo I)
    # ========================================================================
    cfdis_ingreso = []
    cfdis_pago = []
    payments = []
    doctos_relacionados = []
    total = 0
    # Crear 2 ingresos
    for i in range(2):
        total_ingreso = Decimal(str(random.randint(100, 500)))
        fecha = datetime(2025, 1, 15 + i)
        ingreso = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=fecha,
            FechaFiltro=fecha,
            PaymentDate=fecha,
            Moneda="MXN",
            TipoDeComprobante="I",
            ExcludeFromISR=False,
            Estatus=True,
            is_issued=True,
            Total=total_ingreso,
            TotalMXN=total_ingreso,
            Subtotal=Decimal("0"),
            SubtotalMNX=Decimal("0"),
            Serie=f"I{i:03d}",
            Folio=f"{2000 + i}",
            active=True,
            RfcEmisor="XAXX010101000",
            NombreEmisor=f"PROVEEDOR {i} SA DE CV",
        )
        total += total_ingreso
        cfdis_ingreso.append(ingreso)

    company_session.add_all(cfdis_ingreso)
    company_session.flush()  # Obtener UUIDs

    for i in range(2):
        # Crear CFDI de Pago por egreso
        cfdi_pago = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=cfdis_ingreso[i].Fecha,
            FechaFiltro=cfdis_ingreso[i].Fecha,
            PaymentDate=cfdis_ingreso[i].Fecha,
            Moneda="MXN",
            TipoDeComprobante="P",
            ExcludeFromISR=False,
            Estatus=True,
            is_issued=True,
            Total=cfdis_ingreso[i].Total,
            SubTotalMXN=Decimal("0"),
            Serie=f"P{i:03d}",
            Folio=f"{3000 + i}",
            Version="4.0",
        )

        cfdis_pago.append(cfdi_pago)

        payment_id = identifier_default_factory()
        payment = Payment(
            identifier=payment_id,
            company_identifier=company.identifier,
            is_issued=True,
            uuid_origin=cfdi_pago.UUID,
            index=0,
            FechaPago=cfdis_ingreso[i].Fecha,
            FormaDePagoP="03",
            MonedaP="MXN",
            Monto=cfdis_ingreso[i].Total,
            TipoCambioP=Decimal("1.0"),
            Estatus=True,
        )
        payments.append(payment)

        docto = DoctoRelacionado.demo(
            company_identifier=company.identifier,
            is_issued=True,
            payment_identifier=payment_id,
            UUID=cfdi_pago.UUID,
            UUID_related=cfdis_ingreso[i].UUID,
            FechaPago=fecha,
            MonedaDR="MXN",
            EquivalenciaDR=Decimal("1.0"),
            NumParcialidad=i + 1,
            ImpPagado=cfdis_ingreso[i].Total,
            ImpPagadoMXN=cfdis_ingreso[i].Total,
            active=True,
            Estatus=True,
        )
        doctos_relacionados.append(docto)

    company_session.add_all(cfdis_pago)
    company_session.add_all(payments)
    company_session.add_all(doctos_relacionados)
    company_session.commit()

    company_schema = company.identifier
    domain = [
        ["company_identifier", "=", f"{company_schema}"],
        ["TipoDeComprobante", "in", ["P"]],
        ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
        ["FechaFiltro", "<", "2025-02-01T00:00:00.000"],
        ["is_issued", "=", True],
        ["Estatus", "=", True],
        ["payments.FormaDePagoP", "=", "03"],
    ]

    fuzzy_search = ""
    resume_type = ResumeType.PAYMENT_WITH_DOCTOS

    resultado = CFDIController.resume(
        domain, fuzzy_search, session=company_session, resume_type=resume_type
    )

    # Verificar que el filtro avanzado con formas de pago funciona correctamente
    assert resultado["filtered"]["count"] == 2
    assert resultado["filtered"]["Total"] == total
    assert resultado["filtered"]["total_docto_relacionados"] == total
    assert resultado["excercise"]["count"] == 2
    assert resultado["excercise"]["Total"] == total
    assert resultado["excercise"]["total_docto_relacionados"] == total


def test_resume_received_advance_payment_filter(company_session: Session, company: Company):
    random.seed(42)

    # ========================================================================
    # 1. Crear CFDIs de Egreso (tipo E)
    # ========================================================================
    cfdis_egreso = []
    cfdis_pago = []
    payments = []
    doctos_relacionados = []
    total = 0
    # Crear 2 egresos
    for i in range(2):
        total_egreso = Decimal(str(random.randint(100, 500)))
        fecha = datetime(2025, 1, 15 + i)
        egreso = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=fecha,
            FechaFiltro=fecha,
            PaymentDate=fecha,
            Moneda="MXN",
            TipoDeComprobante="E",  # Egreso (nota de crédito)
            ExcludeFromISR=False,
            Estatus=True,
            is_issued=False,
            Total=total_egreso,
            TotalMXN=total_egreso,
            Subtotal=Decimal("0"),
            SubtotalMNX=Decimal("0"),
            Serie=f"E{i:03d}",
            Folio=f"{2000 + i}",
        )
        total += total_egreso
        cfdis_egreso.append(egreso)

    company_session.add_all(cfdis_egreso)
    company_session.flush()  # Obtener UUIDs

    for i in range(2):
        # Crear CFDI de Pago por egreso
        cfdi_pago = CFDI.demo(
            company_identifier=company.identifier,
            Fecha=cfdis_egreso[i].Fecha,
            FechaFiltro=cfdis_egreso[i].Fecha,
            PaymentDate=cfdis_egreso[i].Fecha,
            Moneda="MXN",
            TipoDeComprobante="P",
            ExcludeFromISR=False,
            Estatus=True,
            is_issued=False,
            Total=cfdis_egreso[i].Total,
            SubTotalMXN=Decimal("0"),
            Serie=f"P{i:03d}",
            Folio=f"{3000 + i}",
            Version="4.0",
        )

        cfdis_pago.append(cfdi_pago)

        payment_id = identifier_default_factory()
        payment = Payment(
            identifier=payment_id,
            company_identifier=company.identifier,
            is_issued=False,
            uuid_origin=cfdi_pago.UUID,
            index=0,
            FechaPago=cfdis_egreso[i].Fecha,
            FormaDePagoP="03",
            MonedaP="MXN",
            Monto=cfdis_egreso[i].Total,
            TipoCambioP=Decimal("1.0"),
            Estatus=True,
        )
        payments.append(payment)

        docto = DoctoRelacionado.demo(
            company_identifier=company.identifier,
            is_issued=False,
            payment_identifier=payment_id,
            UUID=cfdi_pago.UUID,
            UUID_related=cfdis_egreso[i].UUID,
            FechaPago=fecha,
            MonedaDR="MXN",
            EquivalenciaDR=Decimal("1.0"),
            NumParcialidad=i + 1,
            ImpPagado=cfdis_egreso[i].Total,
            ImpPagadoMXN=cfdis_egreso[i].Total,
            active=True,
            Estatus=True,
        )
        doctos_relacionados.append(docto)

    company_session.add_all(cfdis_pago)
    company_session.add_all(payments)
    company_session.add_all(doctos_relacionados)
    company_session.commit()

    company_schema = company.identifier
    domain = [
        ["company_identifier", "=", f"{company_schema}"],
        ["TipoDeComprobante", "in", ["P"]],
        ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
        ["FechaFiltro", "<", "2025-02-01T00:00:00.000"],
        ["is_issued", "=", False],
        ["Estatus", "=", True],
        ["payments.FormaDePagoP", "=", "03"],
    ]

    fuzzy_search = ""
    resume_type = ResumeType.PAYMENT_WITH_DOCTOS

    resultado = CFDIController.resume(
        domain, fuzzy_search, session=company_session, resume_type=resume_type
    )

    # Verificar que el filtro avanzado con formas de pago funciona correctamente
    assert resultado["filtered"]["count"] == 2
    assert resultado["filtered"]["Total"] == total
    assert resultado["filtered"]["total_docto_relacionados"] == total
    assert resultado["excercise"]["count"] == 2
    assert resultado["excercise"]["Total"] == total
    assert resultado["excercise"]["total_docto_relacionados"] == total
