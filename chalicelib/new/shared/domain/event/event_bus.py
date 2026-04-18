import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta

from chalicelib.logger import DEBUG, EXCEPTION, WARNING, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.shared.domain.event.event import DomainEvent
from chalicelib.new.shared.domain.event.event_handler import EventHandler
from chalicelib.new.shared.domain.event.event_type import EventType


@dataclass
class EventBus:
    handlers: dict[EventType, list[EventHandler]] = field(default_factory=lambda: defaultdict(list))
    event_count: dict[EventType, int] = field(default_factory=lambda: defaultdict(int))
    last_event: DomainEvent | None = None
    sleep_before_publish = timedelta(seconds=0)
    lock = threading.Lock()

    def subscribe(self, *, handler: EventHandler, event_type: EventType):
        self.handlers[event_type].append(handler)

    def publish(self, event_type: EventType, event: DomainEvent):
        self.event_count[event_type] += 1
        self.last_event = event
        if not self.handlers[event_type]:
            log(
                Modules.BUS,
                WARNING,
                "NO_HANDLERS",
                {
                    "event_type": event_type,
                },
            )
        sleep_time = self.sleep_before_publish.total_seconds()
        if sleep_time > 0:
            log(
                Modules.BUS,
                DEBUG,
                "SLEEP_BEFORE_PUBLISH",
                {
                    "sleep_time": sleep_time,
                    "event": event,
                },
            )
            time.sleep(sleep_time)

        if envars.LOCAL_INFRA:
            self._publish_in_parallel(event_type, event)
        else:
            self._publish(event_type, event)

    def _publish(self, event_type: EventType, event: DomainEvent):
        for handler in self.handlers[event_type]:
            try:
                log(
                    Modules.BUS,
                    DEBUG,
                    "PUBLISHING_EVENT",
                    {
                        "event": event,
                        "handler": handler,
                    },
                )
                handler.handle(event)
            except Exception as e:
                log(
                    Modules.BUS,
                    EXCEPTION,
                    "HANDLER_FAILED",
                    {
                        "event": event,
                        "handler": handler,
                        "exception": e,
                    },
                )

    def _publish_in_parallel(self, event_type: EventType, event: DomainEvent):
        def publish_event():
            with self.lock:
                for handler in self.handlers[event_type]:
                    handler.handle(event)

        thread = threading.Thread(target=publish_event)
        thread.start()

    def get_event_count(self, event_type: EventType):
        return self.event_count[event_type]
