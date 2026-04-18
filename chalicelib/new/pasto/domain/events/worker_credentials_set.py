from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message.sqs_message import SQSMessage


class WorkerCredentialsSet(SQSMessage):
    workspace_identifier: Identifier
    worker_id: str
    worker_token: str
