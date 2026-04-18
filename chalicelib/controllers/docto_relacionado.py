from chalice import BadRequestError
from sqlalchemy.orm import Session

from chalicelib.controllers.common import CommonController
from chalicelib.logger import log_in
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models.tenant import DoctoRelacionado as DoctoRelacionadoORM


class DoctoRelacionadoController(CommonController):
    model = DoctoRelacionadoORM
    _order_by = "Folio"

    def set_exclude_from_iva(self, uuids: dict[Identifier, bool], session: Session):
        uuids_to_update = set(uuids.keys())
        log_in(uuids_to_update)
        filters = (DoctoRelacionadoORM.identifier.in_(uuids_to_update),)
        records = session.query(DoctoRelacionadoORM).filter(*filters).all()

        identifiers_in_records = {record.identifier for record in records}

        if identifiers_in_records != uuids_to_update:
            raise BadRequestError("One or more relations are not in the database.")

        for record in records:
            record.ExcludeFromIVA = uuids[record.identifier]

        return {
            "result": "ok",
        }
