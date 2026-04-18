import uuid

Identifier = str


def normalize_identifier(identifier: Identifier) -> Identifier:
    return str(uuid.UUID(identifier))


def identifier_default_factory():
    return str(uuid.uuid4())
