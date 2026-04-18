import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session

from chalicelib.new.cfdi_status_logger import CFDIStatusLog, get_cfdi_status_log
from chalicelib.new.query.domain import DownloadType, RequestType
from chalicelib.new.query.domain.enums import QueryState
from chalicelib.new.query.domain.query_creator import QueryCreator
from chalicelib.new.query.infra.query_repository_sa import QueryRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI


def creacion_peticiones_metadata_scrap(
    company_session: Session, session: Session, company: Company
):
    query_repo = QueryRepositorySA(session=company_session)
    creator = QueryCreator(query_repo=query_repo, session=session)
    creator.create(
        company_identifier=company.identifier,
        download_type=DownloadType.ISSUED,
        request_type=RequestType.METADATA,
        state=QueryState.PROCESSED,
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 2),
        is_manual=False,
        wid=1,
        cid=1,
    )
    creator.create(
        company_identifier=company.identifier,
        download_type=DownloadType.RECEIVED,
        request_type=RequestType.METADATA,
        state=QueryState.PROCESSED,
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 2),
        is_manual=False,
        wid=1,
        cid=1,
    )
    creator.create(
        company_identifier=company.identifier,
        download_type=DownloadType.ISSUED,
        request_type=RequestType.CFDI,
        state=QueryState.PROCESSING,
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 1),
        is_manual=False,
        wid=1,
        cid=1,
    )
    creator.create(
        company_identifier=company.identifier,
        download_type=DownloadType.RECEIVED,
        request_type=RequestType.CFDI,
        state=QueryState.PROCESSING,
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 1),
        is_manual=False,
        wid=1,
        cid=1,
    )
    creator.create(
        company_identifier=company.identifier,
        download_type=DownloadType.ISSUED,
        request_type=RequestType.CFDI,
        state=QueryState.PROCESSED,
        start=datetime(2022, 1, 3),
        end=datetime(2022, 1, 4),
        is_manual=False,
        wid=1,
        cid=1,
    )
    creator.create(
        company_identifier=company.identifier,
        download_type=DownloadType.RECEIVED,
        request_type=RequestType.CFDI,
        state=QueryState.PROCESSED,
        start=datetime(2022, 1, 3),
        end=datetime(2022, 1, 4),
        is_manual=False,
        wid=1,
        cid=1,
    )
    creator.create(
        company_identifier=company.identifier,
        download_type=DownloadType.BOTH,
        request_type=RequestType.BOTH,
        state=QueryState.PROCESSING,
        start=datetime(2022, 1, 4, 0, 1),
        end=datetime(2022, 1, 4, 23, 59),
        is_manual=False,
        wid=1,
        cid=1,
    )


def create_cfdis(company_session: Session):
    cfdis = []
    cfdis.append(
        CFDI.demo(
            is_issued=True,
            FechaFiltro=datetime(2022, 2, 1),
            Fecha=datetime(2022, 1, 1),
            UUID=str(uuid.uuid4()),
            RfcEmisor="EMISOR010101000",
            RfcReceptor="RECEPTOR010101000",
            BaseIVA0=0,
            BaseIVA16=0,
            BaseIVA8=0,
            BaseIVAExento=0,
            IVATrasladado16=0,
            IVATrasladado8=0,
            Total=Decimal("0.00"),
            SubTotal=0,
            TipoCambio=0,
            Neto=0,
            TrasladosIVA=0,
            TrasladosIEPS=0,
            TrasladosISR=0,
            RetencionesIVA=0,
            RetencionesIEPS=0,
            RetencionesISR=0,
            TotalMXN=0,
            SubTotalMXN=0,
            NetoMXN=0,
            TrasladosIVAMXN=0,
            DescuentoMXN=0,
            TrasladosIEPSMXN=0,
            TrasladosISRMXN=0,
            RetencionesIVAMXN=0,
            RetencionesIEPSMXN=0,
            RetencionesISRMXN=0,
            NoCertificado="000000",
            PaymentDate=datetime(2022, 2, 1),
            Descuento=Decimal("0.00"),
            pr_count=Decimal("0"),
            Estatus=True,
            TipoDeComprobante="E",
            FechaCertificacionSat=datetime(2022, 2, 1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            from_xml=True,
        )
    )

    cfdis.append(
        CFDI.demo(
            is_issued=False,
            FechaFiltro=datetime(2022, 3, 1),
            Fecha=datetime(2022, 1, 1),
            UUID=str(uuid.uuid4()),
            RfcEmisor="EMISOR010101000",
            RfcReceptor="RECEPTOR010101000",
            BaseIVA0=0,
            BaseIVA16=0,
            BaseIVA8=0,
            BaseIVAExento=0,
            IVATrasladado16=0,
            IVATrasladado8=0,
            Total=Decimal("0.00"),
            SubTotal=0,
            TipoCambio=0,
            Neto=0,
            TrasladosIVA=0,
            TrasladosIEPS=0,
            TrasladosISR=0,
            RetencionesIVA=0,
            RetencionesIEPS=0,
            RetencionesISR=0,
            TotalMXN=0,
            SubTotalMXN=0,
            NetoMXN=0,
            TrasladosIVAMXN=0,
            DescuentoMXN=0,
            TrasladosIEPSMXN=0,
            TrasladosISRMXN=0,
            RetencionesIVAMXN=0,
            RetencionesIEPSMXN=0,
            RetencionesISRMXN=0,
            NoCertificado="000000",
            PaymentDate=datetime(2022, 3, 1),
            Descuento=Decimal("0.00"),
            pr_count=Decimal("0"),
            TipoDeComprobante="E",
            Estatus=True,
            FechaCertificacionSat=datetime(2022, 3, 1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            from_xml=True,
        )
    )
    cfdis.append(
        CFDI.demo(
            is_issued=True,
            FechaFiltro=datetime(2022, 2, 1),
            Fecha=datetime(2022, 1, 1),
            UUID=str(uuid.uuid4()),
            RfcEmisor="EMISOR010101000",
            RfcReceptor="RECEPTOR010101000",
            BaseIVA0=0,
            BaseIVA16=0,
            BaseIVA8=0,
            BaseIVAExento=0,
            IVATrasladado16=0,
            IVATrasladado8=0,
            Total=Decimal("0.00"),
            SubTotal=0,
            TipoCambio=0,
            Neto=0,
            TrasladosIVA=0,
            TrasladosIEPS=0,
            TrasladosISR=0,
            RetencionesIVA=0,
            RetencionesIEPS=0,
            RetencionesISR=0,
            TotalMXN=0,
            SubTotalMXN=0,
            NetoMXN=0,
            TrasladosIVAMXN=0,
            DescuentoMXN=0,
            TrasladosIEPSMXN=0,
            TrasladosISRMXN=0,
            RetencionesIVAMXN=0,
            RetencionesIEPSMXN=0,
            RetencionesISRMXN=0,
            NoCertificado="000000",
            PaymentDate=datetime(2022, 2, 1),
            Descuento=Decimal("0.00"),
            pr_count=Decimal("0"),
            Estatus=True,
            TipoDeComprobante="E",
            FechaCertificacionSat=datetime(2022, 2, 1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            from_xml=True,
        )
    )
    cfdis.append(
        CFDI.demo(
            is_issued=True,
            FechaFiltro=datetime(2022, 2, 1),
            Fecha=datetime(2022, 1, 2),
            UUID=str(uuid.uuid4()),
            RfcEmisor="EMISOR010101000",
            RfcReceptor="RECEPTOR010101000",
            BaseIVA0=0,
            BaseIVA16=0,
            BaseIVA8=0,
            BaseIVAExento=0,
            IVATrasladado16=0,
            IVATrasladado8=0,
            Total=Decimal("0.00"),
            SubTotal=0,
            TipoCambio=0,
            Neto=0,
            TrasladosIVA=0,
            TrasladosIEPS=0,
            TrasladosISR=0,
            RetencionesIVA=0,
            RetencionesIEPS=0,
            RetencionesISR=0,
            TotalMXN=0,
            SubTotalMXN=0,
            NetoMXN=0,
            TrasladosIVAMXN=0,
            DescuentoMXN=0,
            TrasladosIEPSMXN=0,
            TrasladosISRMXN=0,
            RetencionesIVAMXN=0,
            RetencionesIEPSMXN=0,
            RetencionesISRMXN=0,
            NoCertificado="000000",
            PaymentDate=datetime(2022, 2, 1),
            Descuento=Decimal("0.00"),
            pr_count=Decimal("0"),
            Estatus=True,
            TipoDeComprobante="E",
            FechaCertificacionSat=datetime(2022, 2, 1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            from_xml=False,
        )
    )

    cfdis.append(
        CFDI.demo(
            is_issued=False,
            FechaFiltro=datetime(2022, 3, 1),
            Fecha=datetime(2022, 1, 2),
            UUID=str(uuid.uuid4()),
            RfcEmisor="EMISOR010101000",
            RfcReceptor="RECEPTOR010101000",
            BaseIVA0=0,
            BaseIVA16=0,
            BaseIVA8=0,
            BaseIVAExento=0,
            IVATrasladado16=0,
            IVATrasladado8=0,
            Total=Decimal("0.00"),
            SubTotal=0,
            TipoCambio=0,
            Neto=0,
            TrasladosIVA=0,
            TrasladosIEPS=0,
            TrasladosISR=0,
            RetencionesIVA=0,
            RetencionesIEPS=0,
            RetencionesISR=0,
            TotalMXN=0,
            SubTotalMXN=0,
            NetoMXN=0,
            TrasladosIVAMXN=0,
            DescuentoMXN=0,
            TrasladosIEPSMXN=0,
            TrasladosISRMXN=0,
            RetencionesIVAMXN=0,
            RetencionesIEPSMXN=0,
            RetencionesISRMXN=0,
            NoCertificado="000000",
            PaymentDate=datetime(2022, 3, 1),
            Descuento=Decimal("0.00"),
            pr_count=Decimal("0"),
            TipoDeComprobante="E",
            Estatus=True,
            FechaCertificacionSat=datetime(2022, 3, 1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            from_xml=False,
        )
    )
    cfdis.append(
        CFDI.demo(
            is_issued=False,
            FechaFiltro=datetime(2022, 3, 1),
            Fecha=datetime(2022, 1, 2),
            UUID=str(uuid.uuid4()),
            RfcEmisor="EMISOR010101000",
            RfcReceptor="RECEPTOR010101000",
            BaseIVA0=0,
            BaseIVA16=0,
            BaseIVA8=0,
            BaseIVAExento=0,
            IVATrasladado16=0,
            IVATrasladado8=0,
            Total=Decimal("0.00"),
            SubTotal=0,
            TipoCambio=0,
            Neto=0,
            TrasladosIVA=0,
            TrasladosIEPS=0,
            TrasladosISR=0,
            RetencionesIVA=0,
            RetencionesIEPS=0,
            RetencionesISR=0,
            TotalMXN=0,
            SubTotalMXN=0,
            NetoMXN=0,
            TrasladosIVAMXN=0,
            DescuentoMXN=0,
            TrasladosIEPSMXN=0,
            TrasladosISRMXN=0,
            RetencionesIVAMXN=0,
            RetencionesIEPSMXN=0,
            RetencionesISRMXN=0,
            NoCertificado="000000",
            PaymentDate=datetime(2022, 3, 1),
            Descuento=Decimal("0.00"),
            pr_count=Decimal("0"),
            TipoDeComprobante="E",
            Estatus=True,
            FechaCertificacionSat=datetime(2022, 3, 1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            from_xml=False,
        )
    )
    # dia 4
    cfdis.append(
        CFDI.demo(
            is_issued=False,
            FechaFiltro=datetime(2022, 3, 1),
            Fecha=datetime(2022, 1, 4),
            UUID=str(uuid.uuid4()),
            RfcEmisor="EMISOR010101000",
            RfcReceptor="RECEPTOR010101000",
            BaseIVA0=0,
            BaseIVA16=0,
            BaseIVA8=0,
            BaseIVAExento=0,
            IVATrasladado16=0,
            IVATrasladado8=0,
            Total=Decimal("0.00"),
            SubTotal=0,
            TipoCambio=0,
            Neto=0,
            TrasladosIVA=0,
            TrasladosIEPS=0,
            TrasladosISR=0,
            RetencionesIVA=0,
            RetencionesIEPS=0,
            RetencionesISR=0,
            TotalMXN=0,
            SubTotalMXN=0,
            NetoMXN=0,
            TrasladosIVAMXN=0,
            DescuentoMXN=0,
            TrasladosIEPSMXN=0,
            TrasladosISRMXN=0,
            RetencionesIVAMXN=0,
            RetencionesIEPSMXN=0,
            RetencionesISRMXN=0,
            NoCertificado="000000",
            PaymentDate=datetime(2022, 3, 1),
            Descuento=Decimal("0.00"),
            pr_count=Decimal("0"),
            TipoDeComprobante="E",
            Estatus=True,
            FechaCertificacionSat=datetime(2022, 3, 1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            from_xml=False,
        )
    )
    cfdis.append(
        CFDI.demo(
            is_issued=False,
            FechaFiltro=datetime(2022, 3, 1),
            Fecha=datetime(2022, 1, 4),
            UUID=str(uuid.uuid4()),
            RfcEmisor="EMISOR010101000",
            RfcReceptor="RECEPTOR010101000",
            BaseIVA0=0,
            BaseIVA16=0,
            BaseIVA8=0,
            BaseIVAExento=0,
            IVATrasladado16=0,
            IVATrasladado8=0,
            Total=Decimal("0.00"),
            SubTotal=0,
            TipoCambio=0,
            Neto=0,
            TrasladosIVA=0,
            TrasladosIEPS=0,
            TrasladosISR=0,
            RetencionesIVA=0,
            RetencionesIEPS=0,
            RetencionesISR=0,
            TotalMXN=0,
            SubTotalMXN=0,
            NetoMXN=0,
            TrasladosIVAMXN=0,
            DescuentoMXN=0,
            TrasladosIEPSMXN=0,
            TrasladosISRMXN=0,
            RetencionesIVAMXN=0,
            RetencionesIEPSMXN=0,
            RetencionesISRMXN=0,
            NoCertificado="000000",
            PaymentDate=datetime(2022, 3, 1),
            Descuento=Decimal("0.00"),
            pr_count=Decimal("0"),
            TipoDeComprobante="E",
            Estatus=True,
            FechaCertificacionSat=datetime(2022, 3, 1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            from_xml=False,
        )
    )
    company_session.add_all(cfdis)


def test_get_bitacora_log(company_session: Session, session: Session, company: Company):
    creacion_peticiones_metadata_scrap(
        company_session=company_session, session=session, company=company
    )
    create_cfdis(company_session=company_session)

    start_date = date.fromisoformat("2022-01-01")
    end_date = date.fromisoformat("2022-01-04")

    log = get_cfdi_status_log(
        session=company_session,
        start_date=start_date,
        end_date=end_date,
    )

    days = log["days"]

    # evaluar cantidad de dias solicitados
    assert len(days) == len(
        [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    )

    # dia uno metadata procesada y xml en proceso, pero toal cfdis completo STATE COMPLETE
    assert CFDIStatusLog(days[3]["status"]) == CFDIStatusLog.COMPLETE
    # dia dos metadata procesada y xml procesado, pero toal cfdis incompleto STATE INCOMPLETE
    assert CFDIStatusLog(days[2]["status"]) == CFDIStatusLog.INCOMPLETE
    # dia tres sin metadata y xml procesado, pero con la nueva logica cuenta como cobertura -> EMPTY (0 cfdis)
    assert CFDIStatusLog(days[1]["status"]) == CFDIStatusLog.EMPTY
    # dia cuatro sin metadata y xml en proceso -> INCOMPLETE (ya no usamos IN_PROGRESS)
    assert CFDIStatusLog(days[0]["status"]) == CFDIStatusLog.INCOMPLETE
