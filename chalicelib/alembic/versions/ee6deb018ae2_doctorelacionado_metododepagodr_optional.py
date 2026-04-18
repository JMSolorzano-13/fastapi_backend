"""DoctoRelacionado.MetodoDePagoDR optional

Revision ID: ee6deb018ae2
Revises: 624bf22c36c7
Create Date: 2024-10-07 11:43:58.625240

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ee6deb018ae2"
down_revision = "624bf22c36c7"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column("payment_relation", "MetodoDePagoDR", existing_type=sa.VARCHAR(), nullable=True)


def downgrade():
    op.alter_column(
        "payment_relation", "MetodoDePagoDR", existing_type=sa.VARCHAR(), nullable=False
    )
