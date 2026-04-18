"""add colum pr_count cfdi

Revision ID: dcf947b19da2
Revises: 84c3a8e301b6
Create Date: 2024-04-08 16:58:56.604768

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "dcf947b19da2"
down_revision = "84c3a8e301b6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("cfdi", sa.Column("pr_count", sa.Integer(), nullable=False, server_default="0"))


def downgrade():
    op.drop_column("cfdi", "pr_count")
