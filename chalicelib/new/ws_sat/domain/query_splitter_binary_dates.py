from dataclasses import dataclass

from chalicelib.new.config.infra.envars.control import NUM_QUERY_SPLITS
from chalicelib.new.query.domain.query import Query
from chalicelib.new.query.domain.query_creator import QueryCreator


@dataclass
class QuerySplitterBinaryDates:
    query_creator: QueryCreator

    def split(self, query: Query) -> list[Query]:
        # Se dividió el rango total de tiempo entre la variable NUM_QUERY_SPLITS por defecto es 64.
        split_duration = (query.end - query.start) / NUM_QUERY_SPLITS

        # Usamos el query_creator duplicando la consulta original ajustando los intervalos de tiempo
        queries = [
            self.query_creator.duplicate(
                query,
                start=query.start + (split_duration * i),
                end=query.start + (split_duration * (i + 1)),
            )
            for i in range(NUM_QUERY_SPLITS)
        ]
        # Se ajustó el `end` del último split para que coincida exactamente con el `end` original.
        # Esto se hace para corregir posibles errores de redondeo al dividir el intervalo de tiempo.
        queries[-1] = self.query_creator.duplicate(
            queries[-1], start=queries[-1].start, end=query.end
        )

        return list(queries)
