from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class SQSClientLocal:
    function: Callable

    def send_message(self, QueueUrl: str, MessageBody: str, **kwargs):
        self.function({"Records": [{"body": MessageBody, "receiptHandle": None}]}, {})
