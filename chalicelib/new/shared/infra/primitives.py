from sqlalchemy.dialects.postgresql import UUID


def IdentifierORM():
    return UUID(as_uuid=False)
