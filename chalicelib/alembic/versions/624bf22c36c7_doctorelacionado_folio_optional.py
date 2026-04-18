"""DoctoRelacionado.Folio optional

Revision ID: 624bf22c36c7
Revises: ab1a0c380515
Create Date: 2024-10-07 11:39:53.810910

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "624bf22c36c7"
down_revision = "ab1a0c380515"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column("payment_relation", "Folio", existing_type=sa.VARCHAR(), nullable=True)


def downgrade():
    op.alter_column("payment_relation", "Folio", existing_type=sa.VARCHAR(), nullable=False)
