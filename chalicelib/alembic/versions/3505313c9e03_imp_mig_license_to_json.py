"""[IMP] mig license to json

Revision ID: 3505313c9e03
Revises: 203b4298d1e3
Create Date: 2023-10-16 15:08:54.047739

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "3505313c9e03"
down_revision = "203b4298d1e3"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_index(op.f("ix_workspace_license"), table_name="workspace")
    op.execute("ALTER TABLE workspace ALTER COLUMN license TYPE JSON USING license::JSON")


def downgrade():
    op.execute("ALTER TABLE workspace ALTER COLUMN license TYPE TEXT USING license::TEXT")
    op.create_index(op.f("ix_workspace_license"), "workspace", ["license"], unique=False)
