"""EFOS Tenant

Revision ID: dcf93bda5aaf
Revises: 202506041307
Create Date: 2025-06-04 15:26:09.663643

"""

# revision identifiers, used by Alembic.
revision = "202506041300"
down_revision = "dc6b127ee1c8"
branch_labels = None
depends_on = None


def upgrade():
    "Ya no se usa FDW, en su lugar hay objetos compartidos en la misma base de datos"
    return


def downgrade():
    return
