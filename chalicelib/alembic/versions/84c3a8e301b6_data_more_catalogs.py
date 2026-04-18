"""[DATA] more catalogs

Revision ID: 84c3a8e301b6
Revises: bef098e1f688
Create Date: 2024-02-08 10:57:02.260558

"""

import csv

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "84c3a8e301b6"
down_revision = "bef098e1f688"
branch_labels = None
depends_on = None


MODELS = {
    "cat_nom_banco",
    "cat_nom_clave_ent_fed",
    "cat_nom_periodicidad_pago",
    "cat_nom_riesgo_puesto",
    "cat_nom_tipo_contrato",
    "cat_nom_tipo_jornada",
    "cat_nom_tipo_nomina",
    "cat_nom_tipo_regimen",
}


def upgrade():
    for model in MODELS:
        new_table = sa.table(model, sa.column("code", sa.String), sa.column("name", sa.String))
        op.execute(new_table.delete())
        with open(f"chalicelib/data/{model}.csv") as f:
            reader = csv.reader(f, delimiter="|")
            next(reader)
            data = [{"code": row[0], "name": row[1]} for row in reader]
            op.bulk_insert(
                new_table,
                data,
            )


def downgrade():
    for model in MODELS:
        new_table = sa.table(model)
        op.execute(new_table.delete())
