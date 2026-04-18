"""new table cfdi_relation

Revision ID: ef69031d656a
Revises: dcf947b19da2
Create Date: 2024-04-16 12:21:12.419456

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "ef69031d656a"
down_revision = "dcf947b19da2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "cfdi_relation",
        sa.Column("identifier", postgresql.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("uuid_origin", postgresql.UUID(), nullable=False),
        sa.Column("is_issued", sa.Boolean(), nullable=False),
        sa.Column("Estatus", sa.Boolean(), nullable=False),
        sa.Column("TipoDeComprobante", sa.String(), nullable=False),
        sa.Column("uuid_related", postgresql.UUID(), nullable=False),
        sa.Column("TipoRelacion", sa.String(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("identifier", "is_issued"),
    )
    op.create_index(
        op.f("ix_cfdi_relation_created_at"),
        "cfdi_relation",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_cfdi_relation_uuid_origin"),
        "cfdi_relation",
        ["uuid_origin"],
        unique=False,
    )
    op.create_index(
        op.f("ix_cfdi_relation_uuid_related"),
        "cfdi_relation",
        ["uuid_related"],
        unique=False,
    )
    op.create_index(
        op.f("ix_cfdi_relation_TipoRelacion"),
        "cfdi_relation",
        ["TipoRelacion"],
        unique=False,
    )
    op.create_index(
        op.f("ix_cfdi_relation_TipoDeComprobante"),
        "cfdi_relation",
        ["TipoDeComprobante"],
        unique=False,
    )


def downgrade():
    op.drop_table("cfdi_relation")
    op.drop_index(op.f("ix_cfdi_relation_company_identifier"), table_name="cfdi_relation")
    op.drop_index(op.f("ix_cfdi_relation_created_at"), table_name="cfdi_relation")
    op.drop_index(op.f("ix_cfdi_relation_uuid_origin"), table_name="cfdi_relation")
    op.drop_index(op.f("ix_cfdi_relation_uuid_related"), table_name="cfdi_relation")
    op.drop_index(op.f("ix_cfdi_relation_TipoRelacion"), table_name="cfdi_relation")
    op.drop_index(op.f("ix_cfdi_relation_TipoDeComprobante"), table_name="cfdi_relation")
