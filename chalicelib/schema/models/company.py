import json

from pydantic import PostgresDsn
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship

from chalicelib.new.config.infra import envars
from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.workspace import Workspace

from .model import Model

DEFAULT_DATA = {
    "scrap_status_constancy": {"current_status": "", "updated_at": ""},
    "scrap_status_order": {"current_status": "", "updated_at": ""},
}


class Company(Model):
    __tablename__ = "company"

    name = Column(
        String,
        index=True,
        nullable=False,
    )
    workspace_id = Column(  # TODO remove
        Integer,
        ForeignKey("workspace.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    workspace_identifier = Column(
        IdentifierORM(),
        ForeignKey("workspace.identifier", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    rfc = Column(
        String,
        index=True,
    )
    active = Column(
        Boolean,
        default=True,
    )
    have_certificates = Column(
        Boolean,
        default=False,
        index=True,
    )
    has_valid_certs = Column(
        Boolean,
        default=False,
        index=True,
    )
    emails_to_send_efos = Column(
        JSONB,
    )
    emails_to_send_errors = Column(
        JSONB,
    )
    emails_to_send_canceled = Column(
        JSONB,
    )
    historic_downloaded = Column(
        Boolean,
        default=False,
    )
    last_ws_download = Column(
        DateTime,
    )
    exceed_metadata_limit = Column(
        Boolean,
        default=False,
        nullable=False,
    )
    permission_to_sync = Column(
        Boolean,
        default=False,
        nullable=False,
    )
    last_notification = Column(
        DateTime,
    )
    pasto_company_identifier = Column(
        IdentifierORM(),
        ForeignKey("pasto_company.pasto_company_id", ondelete="SET NULL"),
        index=True,
    )
    pasto_last_metadata_sync = Column(
        DateTime,
    )
    add_auto_sync = Column(
        Boolean,
        index=True,
        default=False,
    )
    data: dict = Column(
        MutableDict.as_mutable(JSONB),
        nullable=False,
        server_default=text(f"'{json.dumps(DEFAULT_DATA)}'::jsonb"),
    )
    # Tenant database configuration
    tenant_db_name = Column(String)
    tenant_db_host = Column(String)
    tenant_db_port = Column(Integer, default=5432)
    tenant_db_user = Column(String)
    tenant_db_password = Column(String)  # TODO: encrypt
    tenant_db_schema = Column(String)

    workspace = relationship(  # TODO move later
        Workspace,
        backref="companies",
        foreign_keys=[workspace_identifier],
    )

    def get_emails_sets(self) -> dict[str, set[str]]:
        return {
            "efos": set(self.emails_to_send_efos or []),
            "errors": set(self.emails_to_send_errors or []),
            "canceled": set(self.emails_to_send_canceled or []),
        }

    def is_especial(self) -> bool:
        return self.rfc in envars.SPECIAL_RFCS

    @property
    def tenant_db_url(self) -> str:
        if not all([self.tenant_db_host, self.tenant_db_name, self.tenant_db_user]):
            raise ValueError("Missing tenant database configuration")

        return str(
            PostgresDsn.build(
                scheme="postgresql",
                username=self.tenant_db_user,
                password=self.tenant_db_password,
                host=self.tenant_db_host,
                port=int(self.tenant_db_port) or 5432,
                path=self.tenant_db_name,
            )
        )

    @property
    def tenant_db_url_with_schema(self) -> str:
        if not all([self.tenant_db_host, self.tenant_db_name, self.tenant_db_user]):
            raise ValueError("Missing tenant database configuration")

        return str(
            PostgresDsn.build(
                scheme="postgresql",
                username=self.tenant_db_user,
                password=self.tenant_db_password,
                host=self.tenant_db_host,
                port=int(self.tenant_db_port) or 5432,
                path=f"{self.tenant_db_name}.{self.tenant_db_schema}",
            )
        )
