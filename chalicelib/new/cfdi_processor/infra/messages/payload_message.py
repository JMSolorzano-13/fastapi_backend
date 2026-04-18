from typing import Any

from chalicelib.new.shared.infra.message.sqs_company import SQSCompany


class SQSMessagePayload(SQSCompany):
    json_body: dict[str, Any]
