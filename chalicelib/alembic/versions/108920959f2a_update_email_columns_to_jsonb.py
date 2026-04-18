"""update email columns to jsonb

Revision ID: 108920959f2a
Revises: ff09183f702a
Create Date: 2026-01-16 16:20:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "108920959f2a"
down_revision = "ff09183f702a"
branch_labels = None
depends_on = None


def upgrade():
    # 1. Convertir columnas a JSONB
    # Usamos USING para interpretar cadenas JSON existentes (si las hay),
    # o iniciar vacío si son NULL.
    # Como la consulta maneja los NULL, simplemente podemos convertir
    # las cadenas JSON válidas.
    # Sin embargo, si son cadenas vacías o JSON inválido, esto podría fallar.
    # Se asume que son NULL o cadenas JSON válidas debido a la lógica previa.

    op.alter_column(
        "company",
        "emails_to_send_efos",
        existing_type=sa.String(),
        type_=JSONB,
        postgresql_using="emails_to_send_efos::jsonb",
    )

    op.alter_column(
        "company",
        "emails_to_send_errors",
        existing_type=sa.String(),
        type_=JSONB,
        postgresql_using="emails_to_send_errors::jsonb",
    )

    op.alter_column(
        "company",
        "emails_to_send_canceled",
        existing_type=sa.String(),
        type_=JSONB,
        postgresql_using="emails_to_send_canceled::jsonb",
    )

    # 2. Ejecutamos el update
    op.execute(
        text(
            """
            UPDATE company c
            SET
                emails_to_send_efos = (
                    SELECT
                        CASE
                            WHEN c.emails_to_send_efos IS NULL THEN
                                jsonb_build_array(u.email)
                            WHEN c.emails_to_send_efos @> jsonb_build_array(u.email) THEN
                                c.emails_to_send_efos
                            ELSE
                                c.emails_to_send_efos || jsonb_build_array(u.email)
                        END
                ),
                emails_to_send_errors = (
                    SELECT
                        CASE
                            WHEN c.emails_to_send_errors IS NULL THEN
                                jsonb_build_array(u.email)
                            WHEN c.emails_to_send_errors @> jsonb_build_array(u.email) THEN
                                c.emails_to_send_errors
                            ELSE
                                c.emails_to_send_errors || jsonb_build_array(u.email)
                        END
                ),
                emails_to_send_canceled = (
                    SELECT
                        CASE
                            WHEN c.emails_to_send_canceled IS NULL THEN
                                jsonb_build_array(u.email)
                            WHEN c.emails_to_send_canceled @> jsonb_build_array(u.email) THEN
                                c.emails_to_send_canceled
                            ELSE
                                c.emails_to_send_canceled || jsonb_build_array(u.email)
                        END
                )
            FROM workspace w
            JOIN "user" u ON u.id = w.owner_id
            WHERE c.workspace_id = w.id;
            """
        )
    )


def downgrade():
    # Revert to String
    op.alter_column(
        "company",
        "emails_to_send_canceled",
        existing_type=JSONB,
        type_=sa.String(),
        postgresql_using="emails_to_send_canceled::text",
    )

    op.alter_column(
        "company",
        "emails_to_send_errors",
        existing_type=JSONB,
        type_=sa.String(),
        postgresql_using="emails_to_send_errors::text",
    )

    op.alter_column(
        "company",
        "emails_to_send_efos",
        existing_type=JSONB,
        type_=sa.String(),
        postgresql_using="emails_to_send_efos::text",
    )
