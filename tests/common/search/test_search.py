import random
from datetime import datetime
from decimal import Decimal

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session, aliased

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.controllers.cfdi_export import CfdiExportController
from chalicelib.controllers.common import ensure_fields_labeled
from chalicelib.controllers.company import CompanyController
from chalicelib.controllers.docto_relacionado import DoctoRelacionadoController
from chalicelib.controllers.poliza import PolizaController
from chalicelib.new.shared.domain.primitives import identifier_default_factory
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import DoctoRelacionado, Payment
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.cfdi_export import CfdiExport
from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado


def test_mini_search(session: Session, company: Company):
    records, next_page, total_records = CompanyController.search(
        domain=[
            ["identifier", "=", company.identifier],
        ],
        fields=["identifier"],
        session=session,
    )
    assert len(records) == 1
    assert total_records == len(records)
    assert not next_page


def test_search_cardinality_in_to_many_by_domain(
    session: Session,
    company: Company,
):
    records, next_page, total_records = CompanyController.search(
        domain=[
            ["identifier", "=", company.identifier],
            ["permissions.role", "in", ["OPERATOR", "PAYROLL"]],
        ],
        fields=["name"],
        session=session,
    )
    assert len(records) == 1
    assert total_records == len(records)
    assert not next_page


def test_search_cardinality_in_to_many(session: Session, company: Company):
    records, next_page, total_records = CompanyController.search(
        domain=[
            ["identifier", "=", company.identifier],
        ],
        fields=[
            "name",
            "workspace.name",
            "permissions.role",
            "workspace.identifier",
            "permissions.user.email",
            "created_at",
            "workspace.companies.name",
        ],
        session=session,
    )
    assert len(records) == 1
    assert total_records == len(records)
    assert not next_page
    dict_repr = CompanyController.to_nested_dict(records)
    assert "permissions" in dict_repr[0]
    assert "workspace" in dict_repr[0]
    assert "companies" in dict_repr[0]["workspace"]
    assert len(dict_repr[0]["permissions"]) == 2


def test_crash_count(company_session: Session):
    records, next_page, total_records = CFDIController.search(
        domain=[
            ["FechaFiltro", ">=", "2024-01-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-01-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["MetodoPago", "=", "PPD"],
            ["balance", "<=", 0],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "I"],
        ],
        fields=[
            "FechaCancelacion",
            "cfdi_related.Estatus",
            # "cfdi_related.TipoDeComprobante",
            # "paid_by.UUID",
            # "paid_by.cfdi_related.Estatus",
            # "c_regimen_fiscal_receptor.name",
            # "c_forma_pago.name",
            # "c_metodo_pago.name",
            # "balance",
            # "ExcludeFromIVA",
            # "cfdi_related.uuid_origin",
            # "active_egresos.Total",
        ],
        session=company_session,
    )
    assert total_records == len(records)


def test_search_FechaCancelacion(company_session: Session):
    domain = [
        # ["company_identifier", "=", "46c17bcc-f345-48bd-94a2-897c07d3141c"],
        # ["FechaFiltro", ">=", "2025-09-01T00:00:00.000"],
        # ["FechaFiltro", "<", "2025-10-01T00:00:00.000"],
        # ["is_issued", "=", True],
        # ["TipoDeComprobante", "=", "I"],
    ]
    order_by = ""
    fuzzy_search = ""
    limit = 30
    offset = 0
    fields = [
        # "FechaCancelacion",
        # "from_xml",
        # "TipoDeComprobante",
        # "cfdi_related.Estatus",
        # "cfdi_related.TipoDeComprobante",
        # "paid_by.UUID",
        # "paid_by.cfdi_related.Estatus",
        # "Fecha",
        # "Version",
        # "Serie",
        # "Folio",
        # "RfcReceptor",
        # "NombreReceptor",
        # "RegimenFiscalReceptor",
        # "c_regimen_fiscal_receptor.name",
        # "SubTotal",
        # "SubTotalMXN",
        # "Descuento",
        # "DescuentoMXN",
        # "Neto",
        # "NetoMXN",
        # "RetencionesIVA",
        # "RetencionesIVAMXN",
        # "RetencionesISR",
        # "RetencionesISRMXN",
        # "TrasladosIVA",
        # "TrasladosIVAMXN",
        # "Total",
        # "TotalMXN",
        # "Moneda",
        # "TipoCambio",
        # "UsoCFDIReceptor",
        # "FormaPago",
        # "c_forma_pago.name",
        # "MetodoPago",
        # "c_metodo_pago.name",
        # "CondicionesDePago",
        # "FechaYear",
        # "FechaMonth",
        # "FechaCertificacionSat",
        # "FechaCertificacionSatYear",
        # "FechaCertificacionSatMonth",
        # "RetencionesIEPS",
        # "RetencionesIEPSMXN",
        # "TrasladosIEPS",
        # "TrasladosIEPSMXN",
        # "TrasladosISR",
        # "TrasladosISRMXN",
        # "NoCertificado",
        # "Exportacion",
        # "Periodicidad",
        # "Meses",
        # "Year",
        # "CfdiRelacionados",
        # "LugarExpedicion",
        # "UUID",
        # "balance",
        # "ExcludeFromIVA",
        # "cfdi_related.uuid_origin",
        "active_egresos.Total",
    ]
    records, next_page, total_records = CFDIController.search(
        domain=domain,
        order_by=order_by,
        fuzzy_search=fuzzy_search,
        limit=limit,
        offset=offset,
        fields=fields,
        session=company_session,
    )


def test_subquery_secondary_join(company_session: Session):
    cfdi_origin_x: CFDI = aliased(CFDI, name="cfdi_origin")
    subquery_x = (
        select(
            func.jsonb_agg(
                func.jsonb_build_object(
                    "Total",
                    cfdi_origin_x.Total,
                )
            )
        )
        .select_from(CfdiRelacionado)
        .join(
            cfdi_origin_x,
            and_(
                CfdiRelacionado.uuid_origin == cfdi_origin_x.UUID,
                cfdi_origin_x.Estatus,
            ),
        )
        .where(
            CfdiRelacionado.uuid_related == CFDI.UUID,
            CfdiRelacionado.TipoDeComprobante == "E",
        )
        .correlate(CFDI)
        .scalar_subquery()
    )
    query_x = company_session.query(CFDI.UUID, subquery_x)
    print(str(query_x))

    cfdi_origin = CFDI.active_egresos.alias
    subquery = (
        select(
            func.jsonb_agg(
                func.jsonb_build_object(
                    "Total",
                    cfdi_origin.Total,
                )
            )
        )
        # .select_from(CFDI.active_egresos.property.secondary)
        # .join(cfdi_origin, CFDI.active_egresos.property.secondaryjoin)
        .where(CFDI.active_egresos.property.primaryjoin)
        .correlate(CFDI)
    )
    subquery = subquery.select_from(CFDI.active_egresos.property.secondary).join(
        cfdi_origin, CFDI.active_egresos.property.secondaryjoin
    )
    subquery = subquery.scalar_subquery()
    query = company_session.query(CFDI.UUID, subquery)
    print(str(query))

    assert str(query_x) == str(query)


def test_where(company_session: Session):
    query, count = DoctoRelacionadoController._get_search_query_and_count(
        session=company_session,
        domain=[
            ["cfdi_origin.TipoDeComprobante", "=", "P"],
            ["payment_related.FormaDePagoP", "in", ["02", "03", "04", "05", "06", "28", "29"]],
            ["cfdi_related.UsoCFDIReceptor", "in", ["G01", "G03"]],
            ["cfdi_origin.ExcludeFromISR", "=", False],
            ["cfdi_origin.Estatus", "=", True],
            ["cfdi_origin.is_issued", "=", False],
            ["FechaPago", ">=", "2024-10-01T00:00:00.000"],
            ["FechaPago", "<", "2024-11-01T00:00:00.000"],
        ],
        fields=ensure_fields_labeled(
            [
                "UUID",
            ]
        ),
    )
    print(str(query))


def test_search_results_as_numbers_and_dicts(company_session: Session, company):
    cfdi = CFDI.demo()
    cfdi.TrasladosIEPS = 0
    cfdi.company_identifier = company.identifier
    company_session.add(cfdi)
    company_session.commit()

    records, next_page, total_records = CFDIController.search(
        domain=[],
        fields=[
            "TrasladosIEPS",
            "paid_by.UUID",
        ],
        session=company_session,
    )
    dict_repr = CFDIController.to_nested_dict(records)
    assert isinstance(dict_repr[0]["TrasladosIEPS"], float)
    assert isinstance(dict_repr[0]["paid_by"], list)


def test_search_enum_as_str(company_session: Session, company):
    export = CfdiExport(
        export_data_type=CfdiExport.ExportDataType.CFDI,
    )
    company_session.add(export)
    company_session.commit()

    records, next_page, total_records = CfdiExportController.search(
        domain=[],
        fields=[
            "export_data_type",
        ],
        session=company_session,
    )
    dict_repr = CfdiExportController.to_nested_dict(records)
    assert isinstance(dict_repr[0]["export_data_type"], str)
    assert dict_repr[0]["export_data_type"] in CfdiExport.ExportDataType.__members__


def test_search_same_sub_attribute_no_duplicated_join(company_session: Session, company):
    records, next_page, total_records = PolizaController.search(
        domain=[],
        fields=[
            "relaciones.cfdi_related.TipoDeComprobante",
            "relaciones.cfdi_related.Fecha",
        ],
        session=company_session,
    )
    assert len(records) == 0


def test_search_advance_payment_filter(company_session: Session, company: Company):
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

    order_by = '"Fecha" asc , "UUID" asc'
    fuzzy_search = ""
    limit = 30
    offset = 0
    fields = ["Total", "TotalMXN"]
    records, next_page, total_records = CFDIController.search(
        domain=domain,
        order_by=order_by,
        fuzzy_search=fuzzy_search,
        limit=limit,
        offset=offset,
        fields=fields,
        session=company_session,
    )

    # Verificar que el filtro avanzado con formas de pago funciona correctamente
    assert total_records == len(records)
