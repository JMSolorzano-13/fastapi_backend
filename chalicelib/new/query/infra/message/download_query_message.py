from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message import SQSMessage


class SQSDownloadMessage(SQSMessage):
    query_identifier: Identifier
