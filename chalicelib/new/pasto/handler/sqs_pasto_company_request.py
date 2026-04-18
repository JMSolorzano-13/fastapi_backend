from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message import SQSMessage


class SQSWorkerCredentialsSetMessage(SQSMessage):
    workspace_identifier: Identifier
    worker_id: str
    worker_token: str
