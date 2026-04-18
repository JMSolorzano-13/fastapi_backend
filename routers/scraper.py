"""Scraper routes — SAT PDF scraping and file retrieval.

Ported from: backend/chalicelib/blueprints/scraper.py
2 routes total.
"""

from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session

from chalicelib.bus import get_global_bus
from chalicelib.controllers.pdf_scraper import ScraperController
from chalicelib.new.cfdi_processor.infra.messages.payload_message import SQSMessagePayload
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.domain.primitives import Identifier
from dependencies import (
    get_company_identifier_rw,
    get_db_session,
    get_db_session_rw,
    get_json_body,
)

router = APIRouter(tags=["Scraper"])


@router.post("/scrap_sat_pdf")
def scrap_sat_pdf(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    company_identifier: Identifier = Depends(get_company_identifier_rw),
):
    document_type = body.get("document_type")

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


@router.post("/get_pdf_files")
def get_pdf_files(
    json_body: dict = Depends(get_json_body),
    session: Session = Depends(get_db_session),
):
    company_identifier = json_body.get("company_identifier")
    document_type = json_body.get("document_type")
    export_data = json_body.get("export_data")

    doc_requested = "cf" if document_type == "constancy" else "oc"

    try:
        return ScraperController.get_files_from_s3(doc_requested, company_identifier, export_data)
    except Exception as e:
        return {
            "url_pdf_content": "",
            "url_pdf_download": "",
            "last_update": "",
            "error": str(e),
        }
