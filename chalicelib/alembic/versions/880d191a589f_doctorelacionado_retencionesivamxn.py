"""DoctoRelacionado.RetencionesIVAMXN

Revision ID: 880d191a589f
Revises: ee6deb018ae2
Create Date: 2024-10-08 12:34:40.236905

"""

import sqlalchemy as sa
from alembic import op

import chalicelib

# revision identifiers, used by Alembic.
revision = "880d191a589f"
down_revision = "ee6deb018ae2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "payment_relation",
        sa.Column(
            "RetencionesIVAMXN",
            chalicelib.schema.UserDefinedType.mx_amount.MXAmount(),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column("payment_relation", "RetencionesIVAMXN")
