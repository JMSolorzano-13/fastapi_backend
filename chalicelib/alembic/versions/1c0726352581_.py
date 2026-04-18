"""empty message

Revision ID: 1c0726352581
Revises: dcf947b19da2
Create Date: 2024-05-03 15:21:50.529953

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "1c0726352581"
down_revision = "dcf947b19da2"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("DROP INDEX IF EXISTS ix_cfdi_has_errors;")

    op.execute(
        """
        CREATE INDEX ix_cfdi_has_errors
        ON cfdi((1))
        WHERE "TipoDeComprobante_I_MetodoPago_PPD"
        OR "TipoDeComprobante_I_MetodoPago_PUE"
        OR "TipoDeComprobante_E_MetodoPago_PPD"
        OR "TipoDeComprobante_E_CfdiRelacionados_None"
        """
    )


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_cfdi_has_errors;")
