from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.bus import get_global_bus
from chalicelib.controllers.pdf_scraper import ScraperController
from chalicelib.new.cfdi_processor.infra.messages.payload_message import SQSMessagePayload
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.domain.primitives import Identifier

bp = SuperBlueprint(__name__)


@bp.route("/scrap_sat_pdf", methods=["POST"], cors=common.cors_config, read_only=False)
def scrap_sat_pdf(session: Session, company_identifier: Identifier):
    json_body = bp.current_request.json_body or {}

    document_type = json_body.get("document_type")

    bus = get_global_bus()

    sqs_domain = {
        "company_identifier": company_identifier,
        "document_type": document_type,
    }
    ScraperController.set_scraper_status(
        "pending",
        document_type,
        company_identifier,
        session=session,
    )

    bus.publish(
        EventType.SAT_SCRAP_PDF,
        SQSMessagePayload(json_body=sqs_domain, company_identifier=company_identifier),
    )

    return sqs_domain


@bp.route("/get_pdf_files", methods=["POST"], cors=common.cors_config)
def get_pdf_files(session: Session):
    json_body = bp.current_request.json_body or {}
    company_identifier = json_body.get("company_identifier")
    document_type = json_body.get("document_type")
    export_data = json_body.get("export_data")

    doc_requested = "cf" if document_type == "constancy" else "oc"

    # Recibir el company_identifier - tipo de documento

    try:
        return ScraperController.get_files_from_s3(doc_requested, company_identifier, export_data)
    except Exception as e:
        return {
            "url_pdf_content": "",
            "url_pdf_download": "",
            "last_update": "",
            "error": str(e),
        }
