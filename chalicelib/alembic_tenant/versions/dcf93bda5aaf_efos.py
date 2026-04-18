"""EFOS Tenant

Revision ID: dcf93bda5aaf
Revises: 202506041307
Create Date: 2025-06-04 15:26:09.663643

"""

# revision identifiers, used by Alembic.
revision = "dcf93bda5aaf"
down_revision = "202506041307"
branch_labels = None
depends_on = None


def upgrade():
    "En BD multischema se utilizan los mismos EFOS para todas las empresas"


def downgrade():
    "En BD multischema se utilizan los mismos EFOS para todas las empresas"
