from chalicelib.new.shared.infra.message.sqs_message import SQSMessage


class SQSDelayed(SQSMessage):
    target_sqs_url: str
    original_message_body: str
