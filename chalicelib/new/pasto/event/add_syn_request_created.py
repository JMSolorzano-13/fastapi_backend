from datetime import date

from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message import SQSCompany


class Request(SQSCompany):
    request_identifier: Identifier


class ExternalSync(Request):
    start: date
    end: date


class COIMetadataUploaded(Request):
    launch_sync: bool = True


class ADDDataSync(ExternalSync):
    pasto_company_identifier: Identifier
    pasto_worker_token: str
