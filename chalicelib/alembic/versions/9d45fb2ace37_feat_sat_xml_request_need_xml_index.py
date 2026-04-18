"""feat(sat_xml_request): need xml index

Revision ID: 9d45fb2ace37
Revises: 84c3a8e301b6
Create Date: 2024-04-03 11:31:25.609629

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "9d45fb2ace37"
down_revision = "84c3a8e301b6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "cfdi_Fecha_Estatus_from_xml_is_too_big_idx",
        "cfdi",
        ["Fecha", "Estatus", "from_xml", "is_too_big"],
        unique=False,
        postgresql_where='("Estatus" AND (NOT from_xml) AND (NOT is_too_big))',
    )


def downgrade():
    op.drop_index(
        "cfdi_Fecha_Estatus_from_xml_is_too_big_idx",
        table_name="cfdi",
        postgresql_where='("Estatus" AND (NOT from_xml) AND (NOT is_too_big))',
    )
