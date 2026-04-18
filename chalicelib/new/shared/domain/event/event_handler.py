from typing import Protocol

from chalicelib.new.shared.domain.event.event import DomainEvent


class EventHandler(Protocol):
    def handle(self, event: DomainEvent):
        raise NotImplementedError
