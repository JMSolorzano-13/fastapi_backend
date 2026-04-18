"""[DATA] catalogs

Revision ID: bef098e1f688
Revises: e67bdb913f9f
Create Date: 2024-02-08 10:50:07.876835

"""

import csv

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bef098e1f688"
down_revision = "e67bdb913f9f"
branch_labels = None
depends_on = None

MODELS = {
    "cat_aduana",
    "cat_clave_prod_serv",
    "cat_clave_unidad",
    "cat_exportacion",
    "cat_forma_pago",
    "cat_impuesto",
    "cat_meses",
    "cat_metodo_pago",
    "cat_moneda",
    "cat_objeto_imp",
    "cat_pais",
    "cat_periodicidad",
    "cat_regimen_fiscal",
    "cat_tipo_de_comprobante",
    "cat_tipo_relacion",
    "cat_uso_cfdi",
}


def upgrade():
    for model in MODELS:
        new_table = sa.table(model, sa.column("code", sa.String), sa.column("name", sa.String))
        op.execute(new_table.delete())
        with open(f"chalicelib/data/{model}.csv", encoding="utf-8") as f:
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
