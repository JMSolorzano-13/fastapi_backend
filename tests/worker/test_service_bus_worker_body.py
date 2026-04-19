"""Decode Service Bus received message bodies (DATA generator vs bytes)."""

from __future__ import annotations

from azure.servicebus.amqp import AmqpMessageBodyType

from worker.service_bus_worker import decode_service_bus_received_body


class _FakeDataMsg:
    """Mimic azure-servicebus 7.12+: DATA ``body`` is a generator over chunks."""

    body_type = AmqpMessageBodyType.DATA

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    @property
    def body(self):
        return (c for c in self._chunks)


class _FakeValueMsg:
    body_type = AmqpMessageBodyType.VALUE

    def __init__(self, payload: object) -> None:
        self._payload = payload

    @property
    def body(self):
        return self._payload


def test_decode_data_body_from_generator_joins_chunks() -> None:
    msg = _FakeDataMsg([b'{"ok":', b'true}'])
    assert decode_service_bus_received_body(msg) == '{"ok":true}'


def test_decode_data_body_single_bytes() -> None:
    class M:
        body_type = AmqpMessageBodyType.DATA
        body = b'{"x":1}'

    assert decode_service_bus_received_body(M()) == '{"x":1}'


def test_decode_value_body_str() -> None:
    assert decode_service_bus_received_body(_FakeValueMsg("plain")) == "plain"
