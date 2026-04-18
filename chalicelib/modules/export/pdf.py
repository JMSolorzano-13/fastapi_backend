import json
from collections import defaultdict

import jinja2
import pdfkit  # type: ignore
from flask_qrcode import QRcode  # type: ignore

from chalicelib.config import get_pdfkit_configuration
from chalicelib.new.config.infra import envars
from chalicelib.new.utils.datetime import mx_now
from chalicelib.schema.models.tenant import CFDI
from chalicelib.schema.models.tenant.poliza import Poliza

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader("chalicelib/data/pdf"),
)

CFDI_TEMPLATES_BY_TIPO_DE_COMPROBANTE = defaultdict(
    lambda: "cfdi/common.html.jinja",
    {
        "P": "cfdi/pago.html.jinja",
        "N": "cfdi/nomina.html.jinja",
    },
)

POLIZA_TEMPLATE_PATH = "poliza.html.jinja"

PDFRendered = bytes


def get_cfdi_pdf(record: CFDI) -> PDFRendered:
    template_path = CFDI_TEMPLATES_BY_TIPO_DE_COMPROBANTE[record.TipoDeComprobante]

    template = env.get_template(template_path)

    conceptos = json.loads(record.Conceptos or "{}")
    conceptos = conceptos.get("Concepto", [])
    display_logo = envars.DISPLAY_LOGO_IN_CFDI_PDF
    if not isinstance(conceptos, list):
        conceptos = [conceptos]
    impuestos = json.loads(record.Impuestos or "{}")
    rendered = template.render(
        cfdi=record,
        conceptos=conceptos,
        impuestos=impuestos,
        qrcode=QRcode.qrcode,
        display_logo=display_logo,
    )

    configuration = get_pdfkit_configuration()
    pdf_bytes: PDFRendered = pdfkit.from_string(rendered, configuration=configuration)
    return pdf_bytes


def get_poliza_pdf(record: Poliza) -> PDFRendered:
    template = env.get_template(POLIZA_TEMPLATE_PATH)

    cfdis_relacionados = [relacion.uuid_related for relacion in record.relaciones]
    rendered = template.render(
        poliza=record,
        movimientos=record.movimientos,
        cfdis_relacionados=cfdis_relacionados,
        now=mx_now(),
    )

    configuration = get_pdfkit_configuration()
    pdf_bytes: PDFRendered = pdfkit.from_string(rendered, configuration=configuration)
    return pdf_bytes
