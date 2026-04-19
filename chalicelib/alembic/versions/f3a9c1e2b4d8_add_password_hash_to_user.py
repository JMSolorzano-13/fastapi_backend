"""Add user.password_hash for local JWT / bcrypt (control schema parity).

Revision ID: f3a9c1e2b4d8
Revises: 108920959f2a
Create Date: 2026-04-18

"""

from alembic import op
from sqlalchemy import text

revision = "f3a9c1e2b4d8"
down_revision = "108920959f2a"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'user'
                      AND column_name = 'password_hash'
                ) THEN
                    ALTER TABLE public."user" ADD COLUMN password_hash VARCHAR;
                END IF;
            END $$;
            """
        )
    )


def downgrade():
    op.execute(
        text('ALTER TABLE public."user" DROP COLUMN IF EXISTS password_hash')
    )
