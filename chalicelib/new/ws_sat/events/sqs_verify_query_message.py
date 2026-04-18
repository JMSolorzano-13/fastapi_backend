from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message import SQSMessage


class SQSVerifyQueryMessage(SQSMessage):
    query_identifier: Identifier
