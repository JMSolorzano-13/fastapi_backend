import base64
import datetime
import io
import json
from logging import DEBUG

import boto3
import botocore
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import lambda_client_pdf, s3_client, secretsmanager_get
from chalicelib.bus import get_global_bus
from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.cfdi_processor.infra.messages.payload_message import SQSMessagePayload
from chalicelib.new.config.infra import envars
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.schema.models import Company as CompanyORM

DEPLOY_LAMBDA_SCRAPER_DOCUMENT = "deploy-lambda-scraper-document-sat"

# Lazy initialization to avoid import-time boto3 client creation
_lambda_client = None


def get_lambda_client():
    """Get or create Lambda client with proper configuration."""
    global _lambda_client
    if _lambda_client is None:
        config = botocore.config.Config(
            read_timeout=900,
            connect_timeout=900,
            retries={"max_attempts": 0},
        )
        
        client_kwargs = {
            "config": config,
            "region_name": envars.REGION_NAME,
        }
        
        # Add LocalStack endpoint if in local mode
        if envars.LOCAL_INFRA:
            endpoint_url = envars.AWS_ENDPOINT_URL if hasattr(envars, 'AWS_ENDPOINT_URL') else "http://localhost:4566"
            client_kwargs["endpoint_url"] = endpoint_url
        
        _lambda_client = boto3.client("lambda", **client_kwargs)
    
    return _lambda_client


class ScraperController:
    @classmethod
    def set_scraper_status(
        cls, current_status, document_type, company_identifier, session: Session
    ):
        log(
            Modules.SCRAPER_PDF,
            DEBUG,
            "SETTING_SCRAPER_STATUS",
            {"company_identifier": company_identifier, "document_type": document_type},
        )

        company_data: CompanyORM = (
            session.query(CompanyORM).filter(CompanyORM.identifier == company_identifier).first()
        )

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        company_data.data[f"scrap_status_{document_type}"] = {
            "current_status": current_status,
            "updated_at": now,
        }

        return True

    @classmethod
    def scrap_pdf_from_sat(cls, fiel_repo, company_identifier, document_type):
        log(
            Modules.SCRAPER_PDF,
            DEBUG,
            "SCRAPING_PDF_FROM_SAT",
            {"company_identifier": company_identifier, "document_type": document_type},
        )
        cer, key, txt = fiel_repo._get_files(company_identifier=company_identifier)
        file_prefix = "cf" if document_type == "constancy" else "oc"

        scraper_secrets = secretsmanager_get(DEPLOY_LAMBDA_SCRAPER_DOCUMENT)
        bucket_name = scraper_secrets.get("bucket_name")
        cer = base64.b64encode(cer).decode("utf-8")
        key = base64.b64encode(key).decode("utf-8")
        txt = base64.b64encode(txt).decode("utf-8")

        payload = {
            "cer": cer,
            "key": key,
            "txt": txt,
            "scrap_type": document_type,
        }

        payload = json.dumps(payload)

        response = lambda_client_pdf().invoke(
            FunctionName=scraper_secrets["lambda_scraper"],
            Payload=payload,
        )

        # Archivo recibido
        content = json.loads(response.get("Payload").read().decode("utf-8"))

        log(
            Modules.SCRAPER_PDF,
            DEBUG,
            "CONTENT_RECEIVED",
            {"company_identifier": company_identifier},
        )

        pdf_received = content.get("pdf")

        pdf = base64.b64decode(pdf_received)

        log(
            Modules.SCRAPER_PDF,
            DEBUG,
            "UPLOADING_PDF_TO_S3",
            {"company_identifier": company_identifier},
        )

        s3_client().upload_fileobj(
            io.BytesIO(pdf),
            bucket_name,
            f"{file_prefix}_{company_identifier}.pdf",
        )

        return True

    def get_files_from_s3(doc_requested, company_identifier, export_data):
        file_name = export_data["file_name"]
        EXPIRATION_TIME = 8 * 60 * 60
        scraper_secrets = secretsmanager_get(DEPLOY_LAMBDA_SCRAPER_DOCUMENT)
        bucket_name = scraper_secrets.get("bucket_name")

        # Get file from S3 to extract last modified info
        s3_file = s3_client().head_object(
            Bucket=bucket_name,
            Key=f"{doc_requested}_{company_identifier}.pdf",
        )

        # Get url pdf content in PDF type to send to front and display in browser
        s3_url_content = s3_client().generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket_name,
                "Key": f"{doc_requested}_{company_identifier}.pdf",
                "ResponseContentType": "application/pdf",
                "ResponseContentDisposition": "inline",
            },
            ExpiresIn=EXPIRATION_TIME,
        )

        # Get url pdf to download
        s3_url_download = s3_client().generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket_name,
                "ResponseContentDisposition": f"attachment;filename={file_name}.pdf",
                "Key": f"{doc_requested}_{company_identifier}.pdf",
            },
            ExpiresIn=EXPIRATION_TIME,
        )

        last_update = datetime.datetime.strftime(s3_file.get("LastModified"), "%Y-%m-%d %H:%M:%S")

        return {
            "url_pdf_content": s3_url_content,
            "url_pdf_download": s3_url_download,
            "last_update": last_update,
            "error": "",
        }

    def publish_pdf_scrap_by_document_type(company_identifier, document_type, session):
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

    def trigger_pdf_scraper(company_identifier, session):
        ScraperController.publish_pdf_scrap_by_document_type(
            company_identifier, "constancy", session
        )
        ScraperController.publish_pdf_scrap_by_document_type(company_identifier, "opinion", session)
