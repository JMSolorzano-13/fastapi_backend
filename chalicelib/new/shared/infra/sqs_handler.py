from datetime import timedelta
from logging import INFO
from typing import Any

from pydantic import BaseModel, ConfigDict

from chalicelib.boto3_clients import sqs_client
from chalicelib.logger import DEBUG, ERROR, EXCEPTION, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.shared.domain.primitives import identifier_default_factory
from chalicelib.new.shared.infra.message import SQSMessage
from chalicelib.new.shared.infra.message.sqs_delayed import SQSDelayed


class SQSHandler(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    queue_url: str
    max_delay: timedelta = envars.sqs.MAX_DELAY
    sqs_client: Any | None = None

    def _is_fifo(self) -> bool:
        """Check if the SQS queue is FIFO based on the URL."""
        return self.queue_url.rsplit(".", maxsplit=1)[-1] == "fifo"

    def _get_fifo_parameters(self) -> dict:
        """Generate FIFO-specific parameters for the SQS message."""
        if not self._is_fifo():
            return {}
        return {
            "MessageGroupId": identifier_default_factory(),
            "MessageDeduplicationId": identifier_default_factory(),
        }

    def _get_delay_parameters(self, message: SQSMessage) -> dict:
        """Convert delay time to appropriate SQS parameter format."""
        delay = message.get_delay(max_delay=self.max_delay)
        if delay is None:
            return {}
        log(
            Modules.SQS_HANDLER,
            DEBUG,
            "DELAYING",
            {
                "message": message,
                "queue_url": self.queue_url,
                "delay": delay,
            },
        )

        return {"DelaySeconds": int(delay.total_seconds())}

    def handle(self, message: SQSMessage) -> None:
        """Send a message to the SQS queue with appropriate parameters."""
        log(
            Modules.SQS_HANDLER,
            DEBUG,
            "SENDING_MESSAGE",
            {
                "message": message,
                "queue_url": self.queue_url,
            },
        )
        try:
            fifo_parameters = self._get_fifo_parameters()

            message_body = message.model_dump_json()
            queue_url = self.queue_url

            delay_parameters = self._get_delay_parameters(message)
            if delay_parameters and not isinstance(message, SQSDelayed):
                # Mensaje no `SQSDelayed`, envolverlo en SQSDelayed
                execute_at = message.execute_at
                message.execute_at = None
                message_body = message.model_dump_json()
                log(
                    Modules.SQS_HANDLER,
                    INFO,
                    "DELAYING_MESSAGE",
                    {
                        "message": message,
                        "queue_url": self.queue_url,
                    },
                )

                delayed_message = SQSDelayed(
                    target_sqs_url=self.queue_url,
                    original_message_body=message_body,
                    execute_at=execute_at,
                )
                message_body = delayed_message.model_dump_json()
                queue_url = envars.sqs.DELAYER

            if len(message_body) > envars.sqs.MAX_SQS_MESSAGE_SIZE:
                log(
                    Modules.SQS_HANDLER,
                    ERROR,
                    "MESSAGE_TOO_LARGE",
                    {
                        "message": message,
                        "queue_url": self.queue_url,
                    },
                )
                return

            # Use local SQS client if set, otherwise use default boto3 client
            client = self.sqs_client if self.sqs_client is not None else sqs_client()
            client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                **(fifo_parameters | delay_parameters),
            )
        except Exception as e:
            log(
                Modules.SQS_HANDLER,
                EXCEPTION,
                "SEND_FAILED",
                {
                    "message": message,
                    "queue_url": self.queue_url,
                    "exception": e,
                },
            )
