import random
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, Field, field_validator


@runtime_checkable
class SQSRecord(Protocol):
    """Minimal stub replacing chalice.app.SQSRecord for type compatibility."""

    body: str

from chalicelib.new.shared.domain.event.event import DomainEvent
from chalicelib.new.shared.domain.primitives import Identifier, identifier_default_factory


class SQSMessage(BaseModel, DomainEvent):  # noqa E501
    identifier: Identifier = Field(default_factory=identifier_default_factory, kw_only=True)
    execute_at: datetime | None = None

    @field_validator("execute_at")
    def check_execute_at_is_utc(cls, v):
        if not v:
            return v
        if v.tzinfo and v.tzinfo != UTC:
            raise ValueError("execute_at must be in UTC")
        return v

    def set_delay(self, delay: int | timedelta) -> "SQSMessage":
        """Add a delay (in seconds or timedelta) to the message before it is executed."""
        if isinstance(delay, int):
            delay = timedelta(seconds=delay)
        self.execute_at = datetime.utcnow() + delay
        return self

    def set_random_delay(
        self, max_delay: int | timedelta, min_delay: int | timedelta = 0
    ) -> "SQSMessage":
        """Add a random delay (in seconds or timedelta) to the message before it is executed."""
        max_delay = int(
            max_delay.total_seconds() if isinstance(max_delay, timedelta) else max_delay
        )
        min_delay = int(
            min_delay.total_seconds() if isinstance(min_delay, timedelta) else min_delay
        )
        if min_delay > max_delay:
            min_delay, max_delay = max_delay, min_delay
        delay = timedelta(seconds=random.randint(min_delay, max_delay))
        return self.set_delay(delay)

    def get_delay(self, max_delay: timedelta | None = None) -> timedelta | None:
        """Get the delay until the message is executed."""
        if not self.execute_at:
            return None
        desired = self.execute_at - datetime.utcnow()
        no_delay = timedelta(0)
        desired = max(no_delay, desired)
        return min(desired, max_delay) if max_delay else desired

    def is_ready(self) -> bool:
        """Check if the message is ready to be executed."""
        return not self.execute_at or self.execute_at <= datetime.utcnow()

    @classmethod
    def from_event(cls, event: SQSRecord) -> "SQSMessage":
        return cls.model_validate_json(event.body)
