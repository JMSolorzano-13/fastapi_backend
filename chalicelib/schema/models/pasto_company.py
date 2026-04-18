from sqlalchemy import Column, ForeignKey, String

from chalicelib.new.shared.infra.primitives import IdentifierORM

from .model import BasicModel


class PastoCompany(BasicModel):
    __tablename__ = "pasto_company"

    pasto_company_id = Column(
        IdentifierORM(),
        index=True,
        nullable=False,
        primary_key=True,
    )
    workspace_identifier = Column(
        IdentifierORM(),
        ForeignKey("workspace.identifier", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    name = Column(
        String,
        nullable=False,
    )
    alias = Column(
        String,
        nullable=False,
    )
    rfc = Column(
        String,
        nullable=False,
    )
    bdd = Column(String, server_default="Base de datos no identificada")
    system = Column(
        String,
        server_default="Sistema no identificado",
    )
