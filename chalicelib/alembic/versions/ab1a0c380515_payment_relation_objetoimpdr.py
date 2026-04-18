"""payment relation ObjetoImpDR

Revision ID: ab1a0c380515
Revises: 11444c64c7b4
Create Date: 2024-10-04 13:56:38.413018

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ab1a0c380515"
down_revision = "11444c64c7b4"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("payment_relation", sa.Column("ObjetoImpDR", sa.String(), nullable=True))


def downgrade():
    op.drop_column("payment_relation", "ObjetoImpDR")
