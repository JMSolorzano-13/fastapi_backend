import io
from collections.abc import Iterable
from zipfile import ZipFile

from sqlalchemy.orm import Session
from sqlalchemy.orm.query import Query

from chalicelib.controllers.common import CommonController
from chalicelib.modules.export.pdf import get_poliza_pdf
from chalicelib.schema.models.tenant.poliza import Poliza


class PolizaController(CommonController):
    model = Poliza

    default_read_fields = set()

    def to_pdf(self, query: Iterable[Poliza], fields, session: Session, context) -> bytes:
        # TODO Temporal, cuando haya solo un camino de PDF esto ya no será necesario
        # hay funciones que utilizan una Query con multiples columnas y no solo el objeto Poliza
        # Reemplaza la query original por una que solo traiga Poliza (completo)
        if isinstance(query, Query) and len(query.column_descriptions) != 1:
            query = session.query(Poliza).filter(query.whereclause)

        f = io.BytesIO()
        with ZipFile(f, "w") as zf:
            for record in query:
                pdf = get_poliza_pdf(record)
                # Polizas usan identifier, no UUID
                zf.writestr(f"{record.identifier}.pdf", pdf)
        return f.getvalue()
