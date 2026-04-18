"""Payment & DoctoRelacionado Estatus

Revision ID: 107709c324d3
Revises: 880d191a589f
Create Date: 2024-11-15 13:04:28.413659

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "107709c324d3"
down_revision = "880d191a589f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "payment", sa.Column("Estatus", sa.Boolean(), server_default="TRUE", nullable=False)
    )
    op.add_column(
        "payment_relation",
        sa.Column("Estatus", sa.Boolean(), server_default="TRUE", nullable=False),
    )


def downgrade():
    op.drop_column("payment_relation", "Estatus")
    op.drop_column("payment", "Estatus")
