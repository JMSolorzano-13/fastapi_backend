"""unaccent

Revision ID: e9c67871178d
Revises: cfbfc75ddf2e
Create Date: 2025-06-27 15:11:58.608824

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "e9c67871178d"
down_revision = "cfbfc75ddf2e"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS unaccent;")


def downgrade():
    op.execute("DROP EXTENSION IF EXISTS unaccent;")
