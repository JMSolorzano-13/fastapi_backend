import random
from datetime import datetime

from sqlalchemy.orm import Session

from chalicelib.blueprints.common import get_search_attrs
from chalicelib.controllers.cfdi import CFDIController
from chalicelib.controllers.common_utils.export_xlsx import query_to_xlsx
from chalicelib.controllers.company import CompanyController
from chalicelib.controllers.docto_relacionado import DoctoRelacionadoController
from chalicelib.new.cfdi_processor.domain.enums.cfdi_export_state import CfdiExportState
from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.docto_relacionado import DoctoRelacionado
from chalicelib.schema.models.tenant.payment import Payment
from tests.load_data.test_load_xml import XMLS_PATH, read_files_from_directory


def test_xlsx(company_session: Session):
    # load data
    company_session.add_all(CFDI.demo() for _ in range(10))

    fields = [CFDI.UUID.label("x"), CFDI.TipoDeComprobante, CFDI.Fecha, CFDI.Total]
    query = company_session.query(*fields)
    res = query.all()
    excel = query_to_xlsx(query)
    assert excel.active
    # records are right
    assert len(res) == excel.active.max_row - 1  # -1 for header
    # headers are right
    assert [cell.value for cell in excel.active[1]] == [col.name for col in fields]
    # assert column size is ok
    # assert numeric columns are aligned
    # assert date columns are formatted correctly


def test_xlsx_from_search(company_session: Session):
    # load data
    company_session.add_all(CFDI.demo(Moneda="MXN") for _ in range(10))

    order_by = "RfcEmisor"
    domain = []
    limit = None
    offset = 0
    fuzzy_search = ""
    fields = {
        "UUID": "UUID",
        "TipoDeComprobante": "TipoDeComprobante",
        "Fecha": "Fecha",
        "Total": "Total",
        "c_moneda.code": "Moneda Código",
        "c_moneda.name": "Moneda Nombre",
    }

    query = CFDIController._get_search_query(
        domain=domain,
        order_by=order_by,
        limit=limit,
        offset=offset,
        active=True,
        fuzzy_search=fuzzy_search,
        fields=fields,
        session=company_session,
    )
    res = query.all()
    excel = query_to_xlsx(query)
    assert excel.active
    # records are right
    assert len(res) == excel.active.max_row - 1  # -1 for header
    # headers are right
    assert len(excel.active[1]) == len(fields)
    # assert [cell.value for cell in excel.active[1]] == [col for col in fields]
    # assert column size is ok
    # assert numeric columns are aligned
    # assert date columns are formatted correctly


def test_xlsx_docto(company_session: Session):
    # load data
    random.seed(32)
    cfdis = [
        CFDI.demo(
            Moneda="MXN",
            TipoDeComprobante="P",
            ExcludeFromISR=False,
            Estatus=True,
            is_issued=False,
            UsoCFDIReceptor=random.choice(["G01", "G03"]),
            SubTotalMXN=random.uniform(100, 1000),
            DescuentoMXN=random.uniform(0, 100),
        )
        for _ in range(10)
    ]
    company_session.add_all(cfdis)
    company_session.add_all(
        DoctoRelacionado.demo(
            company_identifier=random.choice(cfdis).company_identifier,
            FechaPago=datetime.now(),
            active=True,
            cfdi_origin=random.choice(cfdis),
            cfdi_related=random.choice(cfdis),
            payment_related=Payment(
                company_identifier=random.choice(cfdis).company_identifier,
                FormaDePagoP=random.choice(["02", "03", "04", "05", "06", "28", "29"]),
                uuid_origin=random.choice(cfdis).UUID,
                index=0,
                FechaPago=datetime.now(),
                MonedaP="MXN",
                Monto=100.0,
            ),
        )
        for _ in range(10)
    )
    company_session.commit()

    order_by = "cfdi_related.UsoCFDIReceptor"
    domain = [
        ["cfdi_origin.TipoDeComprobante", "=", "P"],
        ["payment_related.FormaDePagoP", "in", ["02", "03", "04", "05", "06", "28", "29"]],
        ["cfdi_related.UsoCFDIReceptor", "in", ["G01", "G03"]],
        ["cfdi_origin.ExcludeFromISR", "=", False],
        ["cfdi_origin.Estatus", "=", True],
        ["cfdi_origin.is_issued", "=", False],
    ]
    limit = None
    offset = 0
    fuzzy_search = ""
    fields = {
        "FechaPago": "Fecha de Pago",
        "cfdi_origin.Fecha": "Fecha de emisión",
        "UUID": "UUID",
        "cfdi_origin.Serie": "Serie",
        "cfdi_origin.Folio": "Folio",
        "cfdi_origin.RfcReceptor": "RFC receptor",
        "cfdi_origin.NombreReceptor": "Receptor",
        "cfdi_origin.FormaPago": "Forma de Pago",
        "cfdi_origin.Moneda": "Moneda de pago",
        "cfdi_related.Fecha": "DR - Fecha de emisión",
        "cfdi_related.Serie": "DR - Serie",
        "cfdi_related.Folio": "DR - Folio",
        "cfdi_related.UUID": "DR - UUID",
        "cfdi_related.UsoCFDIReceptor": "DR - Uso de CFDI",
        "ObjetoImpDR": "DR - Objeto de impuesto",
        "MonedaDR": "DR - Moneda",
        "EquivalenciaDR": "DR - Equivalencia",
        "NumParcialidad": "DR - Numero de parcialidad",
        "ImpPagado": "DR - Importe pagado",
        "ImpPagadoMXN": "DR - Importe pagado MXN",
        "BaseIVA16": "DR - Base IVA 16%",
        "BaseIVA8": "DR - Base IVA 8%",
        "BaseIVA0": "DR - Base IVA 0%",
        "BaseIVAExento": "DR - Base IVA Exento",
        "IVATrasladado16": "DR - IVA 16%",
        "IVATrasladado8": "DR - IVA 8%",
        "TrasladosIVAMXN": "DR - IVA Total",
        "BaseIEPS": "DR - Base IEPS",
        "FactorIEPS": "DR - Factor IEPS",
        "TasaOCuotaIEPS": "DR - Tasa o cuota IEPS",
        "ImporteIEPS": "DR - IEPS",
        "RetencionesISR": "DR - Retenciones ISR",
        "RetencionesIVAMXN": "DR - Retenciones IVA",
    }

    query = DoctoRelacionadoController._get_search_query(
        domain=domain,
        order_by=order_by,
        limit=limit,
        offset=offset,
        active=True,
        fuzzy_search=fuzzy_search,
        fields=fields,
        session=company_session,
    )
    print(str(query))
    excel = query_to_xlsx(query)
    assert excel.active


def test_hybrid(company_session: Session, company: Company):
    cfdi_repo = CFDIRepositorySA(company_session)
    xml_content = read_files_from_directory(XMLS_PATH)
    processor = XMLProcessor(cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session)
    processor.process_xml_files(
        company_identifier=company.identifier,
        xmls_contents=xml_content,
        rfc=company.rfc,
    )
    # Test the SQL expression
    query = company_session.query(DoctoRelacionado.UUID, DoctoRelacionado.BaseIEPS).filter()
    print(str(query))
    for dr in query:
        print(dr)


def test_company(session: Session, company: Company):
    json_body = {
        "domain": [["id", "in", [company.id]]],
        "limit": 10000,
        "offset": 0,
        "fields": [
            "workspace.name",
            "workspace.valid_until",
            "workspace.license",
            "workspace.owner.email",
            "workspace.owner.id",
            "workspace.owner.identifier",
            "workspace.id",
            "workspace.identifier",
            "have_certificates",
            "has_valid_certs",
            "created_at",
            "id",
            "permission_to_sync",
            "name",
            "identifier",
            "pasto_company_identifier",
            "rfc",
            "emails_to_send_efos",
            "emails_to_send_errors",
            "emails_to_send_canceled",
            "exceed_metadata_limit",
            "pasto_last_metadata_sync",
            "add_auto_sync",
        ],
    }
    search_attrs = get_search_attrs(json_body)

    records, next_page, total_records = CompanyController.search(
        **search_attrs, context={}, session=session
    )
    CompanyController.to_nested_dict(records)


def test_controller(company_session: Session):
    company_session.add_all(CFDI.demo(Moneda="MXN") for _ in range(10))
    company_session.flush()

    order_by = "RfcEmisor"
    domain = []
    limit = None
    offset = 0
    fuzzy_search = ""
    fields_labeled = {
        "UUID": "Identificador SAT",
        "TipoDeComprobante": "Tipo de Comprobante",
        "Fecha": "Fecha",
        "Total": "Total",
        "c_moneda.code": "Código de Moneda",
        "c_moneda.name": "Nombre de Moneda",
    }
    file_name = "test.xlsx"
    export = CFDIController.generic_xlsx_export(
        company_session=company_session,
        file_name=file_name,
        fields_labeled=fields_labeled,
        domain=domain,
        order_by=order_by,
        limit=limit,
        offset=offset,
        fuzzy_search=fuzzy_search,
    )

    assert export.url
    assert export.state == CfdiExportState.TO_DOWNLOAD
    assert export.file_name == file_name


def test_search(company_session: Session):
    company_session.add_all(CFDI.demo(Moneda="MXN") for _ in range(10))
    company_session.flush()

    order_by = "RfcEmisor"
    domain = []
    limit = None
    offset = 0
    fuzzy_search = ""
    fields = ["UUID", "c_moneda.code", "c_moneda.name"]
    records, next_page, total_records = CFDIController.search(
        order_by=order_by,
        domain=domain,
        limit=limit,
        offset=offset,
        fuzzy_search=fuzzy_search,
        fields=fields,
        context={},
        session=company_session,
    )
    CFDIController.to_nested_dict(records)
