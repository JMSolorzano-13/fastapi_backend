import uuid
from decimal import ROUND_HALF_UP, Decimal

from sqlalchemy.orm import Session

from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.cfdi_excluded import ExcludedCFDIController
from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI as CFDIORM
from chalicelib.schema.models.tenant.docto_relacionado import DoctoRelacionado
from chalicelib.schema.models.tenant.payment import Payment
from tests.load_data.test_company_load import read_files_from_directory

XMLS_PATH = "tests/iva/xml_iva_acreditable"
bp = SuperBlueprint(__name__)


def insertar_cfdis(c_session: Session, company: Company):
    cfdi_repo = CFDIRepositorySA(session=c_session)
    xml_content = read_files_from_directory(XMLS_PATH)

    xmlProcessor = XMLProcessor(cfdi_repo=cfdi_repo, xml_repo=None, company_session=c_session)

    xmlProcessor.process_xml_files(
        company_identifier=company.identifier,
        xmls_contents=xml_content,
        rfc=company.rfc,
    )


class _FakeRequest:
    def __init__(self, json_body, headers=None):
        self.json_body = json_body
        self.headers = headers or {}


def test_auto_exclusion(company_session: Session, company: Company):
    insertar_cfdis(company_session, company)
    company_session.flush()
    # Todos los campos de este controller, NO son ni de CFDI ni de DoctoRelacionado, sino de una UNION de ambos
    domain = [
        ["Estatus", "=", True],
        ["is_issued", "=", False],
        ["TipoDeComprobante", "in", ["I", "E"]],
        ["PaymentDate", ">=", "2025-07-01"],
        ["PaymentDate", "<", "2025-08-01"],
        ["ExcludeFromIVA", "=", False],
        ["MetodoPago", "=", "PUE"],
        ["Version", "=", "4.0"],
    ]
    order_by = '"Fecha" asc'
    fuzzy_search = ""
    limit = 100
    offset = 0
    fields = []

    records, next_page, total_records = ExcludedCFDIController.search(
        session=company_session,
        domain=domain,
        fields=fields,
        order_by=order_by,
        limit=limit,
        offset=offset,
        fuzzy_search=fuzzy_search,
    )

    assert records
    row = records[0]

    iva_usd = Decimal("24.14")
    tc = Decimal("18.8928")

    iva_mxn_2 = iva_usd * tc

    assert row["IVATrasladado16"] == iva_mxn_2.quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
    assert row["IVATrasladado8"] == 0.0
    assert row["RetencionesIVA"] == 0.0

    cfdis = company_session.query(CFDIORM).all()
    assert len(cfdis) == 1
    assert cfdis[0].TotalMXN == row["Total"]


def test_docto_relacionado_excluded_search(company_session: Session, company: Company):
    cfdi_ingreso = CFDIORM.demo(
        company_identifier=company.identifier,
        is_issued=False,
        Estatus=True,
        PaymentDate="2025-07-15",
        Version="4.0",
        MetodoPago="PPD",
        FormaPago="99",
        ExcludeFromIVA=False,
        TipoDeComprobante="I",
        Fecha="2025-07-15",
        FechaFiltro="2025-07-15",
    )

    cfdi_pago = CFDIORM.demo(
        company_identifier=company.identifier,
        is_issued=False,
        Estatus=True,
        PaymentDate="2025-07-15",
        Version="4.0",
        MetodoPago=None,
        FormaPago=None,
        ExcludeFromIVA=False,
        TipoDeComprobante="P",
        Fecha="2025-07-15",
        FechaFiltro="2025-07-15",
    )

    payment = Payment(
        company_identifier=company.identifier,
        uuid_origin=cfdi_pago.UUID,
        FechaPago=cfdi_pago.PaymentDate,
        FormaDePagoP="03",
        index=0,
        MonedaP="MXN",
        Monto=239.0,
        identifier=str(uuid.uuid4()),
    )

    pr = DoctoRelacionado.demo(
        company_identifier=company.identifier,
        payment_identifier=payment.identifier,
        UUID=cfdi_pago.UUID,
        UUID_related=cfdi_ingreso.UUID,
    )

    company_session.add_all([cfdi_ingreso, cfdi_pago, payment, pr])
    company_session.commit()

    domain = [
        ["Estatus", "=", True],
        ["is_issued", "=", False],
        ["TipoDeComprobante", "in", ["I", "E"]],
        ["PaymentDate", ">=", "2025-07-01"],
        ["PaymentDate", "<", "2025-08-01"],
        ["ExcludeFromIVA", "=", False],
        ["Version", "=", "4.0"],
    ]
    order_by = '"Fecha" asc'
    fuzzy_search = ""
    limit = 100
    offset = 0
    fields = []

    records, next_page, total_records = ExcludedCFDIController.search(
        session=company_session,
        domain=domain,
        fields=fields,
        order_by=order_by,
        limit=limit,
        offset=offset,
        fuzzy_search=fuzzy_search,
    )

    # Debe tomar el Metodo de Pago del Ingreso
    assert records[0].MetodoPago == "PPD"

    # Debe tomar la Forma de Pago del Pago (Payment)
    assert records[0].FormaPago == "03"
