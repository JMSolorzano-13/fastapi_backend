from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.orm import Session

from chalicelib.logger import WARNING, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto.canceler import Canceler
from chalicelib.new.pasto.xml_sender import XMLSender
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models import ADDSyncRequest


@dataclass
class ADDDataSender:
    ocp_key: str
    endpoint: str
    url: str
    bucket: str
    expires_in: timedelta

    def send_xmls_and_cancellations(
        self,
        company_session: Session,
        request_identifier: Identifier,
        company_identifier: Identifier,
        pasto_company_identifier: Identifier,
        pasto_worker_token: str,
    ) -> tuple[tuple[str, int, float], tuple[str, int, float]]:
        request: ADDSyncRequest = company_session.query(ADDSyncRequest).get(request_identifier)
        # assert request is not None, f"Request {request_identifier} not found"
        xml_sender = XMLSender(
            ocp_key=self.ocp_key,
            endpoint=self.endpoint,
            url=self.url,
            authorization=None,
            session=company_session,
            bucket=self.bucket,
            expires_in=self.expires_in,
            api_route=envars.ADD_XML_WEBHOOK,
        )
        xml_action, xmls_sent, xmls_to_send_total, xml_url = xml_sender.send_missing(
            request_identifier=request.identifier,
            company_identifier=company_identifier,
            pasto_worker_token=pasto_worker_token,
            pasto_company_identifier=pasto_company_identifier,
            start=request.start,
            end=request.end,
        )
        if xml_action:
            request.pasto_sent_identifier = xml_action
        else:
            log(
                Modules.ADD,
                WARNING,
                "NO_XML_ACTION",
                {
                    "request_identifier": request.identifier,
                },
            )

        request.xmls_to_send = xmls_sent
        request.xmls_to_send_pending = xmls_sent
        request.xmls_to_send_total = xmls_to_send_total

        canceler = Canceler(
            ocp_key=self.ocp_key,
            endpoint=self.endpoint,
            url=self.url,
            authorization=None,
            session=company_session,
            bucket=self.bucket,
            expires_in=self.expires_in,
            api_route=envars.ADD_CANCEL_WEBHOOK,
        )
        cancel_action, cfdis_to_cancel, cfdis_to_cancel_total, cancel_url = canceler.cancel_missing(
            request_identifier=request.identifier,
            company_identifier=company_identifier,
            pasto_company_identifier=pasto_company_identifier,
            pasto_worker_token=pasto_worker_token,
            start=request.start,
            end=request.end,
        )
        if cancel_action:
            request.pasto_cancel_identifier = cancel_action
        else:
            log(
                Modules.ADD,
                WARNING,
                "NO_CANCEL_ACTION",
                {
                    "request_identifier": request.identifier,
                },
            )

        request.cfdis_to_cancel = cfdis_to_cancel
        request.cfdis_to_cancel_pending = cfdis_to_cancel
        request.cfdis_to_cancel_total = cfdis_to_cancel_total

        if cancel_action == "ERROR" or xml_action == "ERROR":
            request.state = ADDSyncRequest.StateEnum.ERROR
        else:
            request.state = ADDSyncRequest.StateEnum.SENT

        return (
            (xml_action, xmls_sent, xmls_to_send_total, xml_url),
            (cancel_action, cfdis_to_cancel, cfdis_to_cancel_total, cancel_url),
        )
