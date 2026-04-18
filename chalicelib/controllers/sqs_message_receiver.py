import json


def decode(record_body: str) -> dict:
    """Decode a message from a queue"""
    return json.loads(record_body)
