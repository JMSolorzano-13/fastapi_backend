"""payment relation Serie, EquivalenciaDR, ImpPagadoMXN

Revision ID: 11444c64c7b4
Revises: 2dcc80f016fc
Create Date: 2024-10-04 13:46:33.953613

"""

import sqlalchemy as sa
from alembic import op

import chalicelib

# revision identifiers, used by Alembic.
revision = "11444c64c7b4"
down_revision = "2dcc80f016fc"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("payment_relation", sa.Column("Serie", sa.String(), nullable=True))
    op.add_column(
        "payment_relation",
        sa.Column("EquivalenciaDR", sa.Numeric()),
    )
    op.add_column(
        "payment_relation",
        sa.Column(
            "ImpPagadoMXN",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            server_default="0",
            nullable=False,
        ),
    )


def downgrade():
    op.drop_column("payment_relation", "ImpPagadoMXN")
    op.drop_column("payment_relation", "EquivalenciaDR")
    op.drop_column("payment_relation", "Serie")
