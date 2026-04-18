"""feat(payment): iva columns

Revision ID: 4b5fc921c5dd
Revises: 4d50af67cde3
Create Date: 2024-10-03 16:47:27.105211

"""

import sqlalchemy as sa
from alembic import op

import chalicelib

# revision identifiers, used by Alembic.
revision = "4b5fc921c5dd"
down_revision = "4d50af67cde3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "payment_relation",
        sa.Column(
            "FechaPago",
            sa.TIMESTAMP(),
            server_default="2024-10-03",
            nullable=False,
        ),
    )
    op.add_column(
        "payment", sa.Column("is_issued", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column(
        "payment_relation",
        sa.Column("is_issued", sa.Boolean(), server_default="false", nullable=False),
    )
    op.add_column(
        "payment_relation",
        sa.Column(
            "BaseIVA16",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "payment_relation",
        sa.Column(
            "BaseIVA8",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "payment_relation",
        sa.Column(
            "BaseIVA0",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "payment_relation",
        sa.Column(
            "BaseIVAExento",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "payment_relation",
        sa.Column(
            "IVATrasladado16",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "payment_relation",
        sa.Column(
            "IVATrasladado8",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "payment_relation",
        sa.Column(
            "TrasladosIVAMXN",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            server_default="0",
            nullable=False,
        ),
    )


def downgrade():
    op.drop_column("payment_relation", "TrasladosIVAMXN")
    op.drop_column("payment_relation", "IVATrasladado8")
    op.drop_column("payment_relation", "IVATrasladado16")
    op.drop_column("payment_relation", "BaseIVAExento")
    op.drop_column("payment_relation", "BaseIVA0")
    op.drop_column("payment_relation", "BaseIVA8")
    op.drop_column("payment_relation", "BaseIVA16")
    op.drop_column("payment_relation", "is_issued")
    op.drop_column("payment", "is_issued")
    op.drop_column("payment_relation", "FechaPago")
