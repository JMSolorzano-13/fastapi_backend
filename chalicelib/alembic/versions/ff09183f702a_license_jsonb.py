"""license_jsonb

Revision ID: ff09183f702a
Revises: 6808a553af10
Create Date: 2026-01-02 13:29:06.389268

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "ff09183f702a"
down_revision = "6808a553af10"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE workspace ALTER COLUMN license TYPE JSONB USING license::JSONB")


def downgrade():
    op.execute("ALTER TABLE workspace ALTER COLUMN license TYPE JSON USING license::JSON")
