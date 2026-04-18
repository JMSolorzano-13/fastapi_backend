"""[ADD] ix_cfdi_add_need_send_xml

Revision ID: 4c0f3febf87b
Revises: fa467f99e90c
Create Date: 2023-11-10 13:13:51.634726

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "4c0f3febf87b"
down_revision = "fa467f99e90c"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        CREATE INDEX ix_cfdi_add_need_send_xml
        ON cfdi("Fecha", from_xml, add_exists)
        WHERE
            from_xml AND NOT add_exists"""
    )


def downgrade():
    op.drop_index("ix_cfdi_add_need_send_xml", table_name="cfdi")
