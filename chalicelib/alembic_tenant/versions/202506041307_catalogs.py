"""setup FDW for central database catalogs

Revision ID: 202506041307
Revises: dc6b127ee1c8
Create Date: 2025-06-04 13:07:00.000000

"""

# revision identifiers, used by Alembic.
revision = "202506041307"
down_revision = "202506041300"
branch_labels = None
depends_on = None


def upgrade():
    "En BD multischema se utilizan los mismos catalogos para todas las empresas"


def downgrade():
    "En BD multischema se utilizan los mismos catalogos para todas las empresas"
