import pytest

from chalicelib.new.shared.domain.event.event_bus import EventBus


@pytest.fixture(scope="function")
def bus() -> EventBus:
    return EventBus()
