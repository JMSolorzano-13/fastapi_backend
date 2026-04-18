"""[ADD] indexes to improve db usage

Revision ID: fa467f99e90c
Revises: 26fbc55e9a11
Create Date: 2023-11-08 17:48:36.579878

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "fa467f99e90c"
down_revision = "26fbc55e9a11"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(op.f("ix_cfdi_Fecha"), "cfdi", ["Fecha"], unique=False)
    op.create_index(op.f("ix_cfdi_from_xml"), "cfdi", ["from_xml"], unique=False)
    op.create_index(op.f("ix_cfdi_from_Estatus"), "cfdi", ["Estatus"], unique=False)
    op.create_index(op.f("ix_cfdi_from_add_exists"), "cfdi", ["add_exists"], unique=False)


def downgrade():
    op.drop_index(op.f("ix_cfdi_from_add_exists"), table_name="cfdi")
    op.drop_index(op.f("ix_cfdi_from_Estatus"), table_name="cfdi")
    op.drop_index(op.f("ix_cfdi_from_xml"), table_name="cfdi")
    op.drop_index(op.f("ix_cfdi_Fecha"), table_name="cfdi")
