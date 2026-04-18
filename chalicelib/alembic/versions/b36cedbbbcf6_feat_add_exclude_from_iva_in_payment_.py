"""feat_add_exclude_from_iva_in_payment_relation

Revision ID: b36cedbbbcf6
Revises: 880d191a589f
Create Date: 2024-11-01 12:27:45.621852

"""

# revision identifiers, used by Alembic.
revision = "b36cedbbbcf6"
down_revision = "880d191a589f"
branch_labels = None
depends_on = None


def upgrade():
    pass
    # op.add_column(
    #     "payment_relation",
    #     sa.Column("ExcludeFromIVA", sa.Boolean(), server_default="FALSE", nullable=False),
    # )


def downgrade():
    pass
    # op.drop_column("payment_relation", "ExcludeFromIVA")
