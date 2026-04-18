"""[ADD] ix_cfdi_ingresos_pue

Revision ID: 7cbf1ea158a4
Revises: c211ab78636e
Create Date: 2023-11-14 15:11:24.547350

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "7cbf1ea158a4"
down_revision = "c211ab78636e"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
    CREATE INDEX ix_cfdi_ingresos_pue ON
    cfdi(is_issued, "TipoDeComprobante", "MetodoPago", created_at)
    WHERE
        "TipoDeComprobante" = 'I'
        AND
        "MetodoPago" = 'PUE'
    """
    )


def downgrade():
    op.drop_index("ix_cfdi_ingresos_pue", table_name="cfdi")
