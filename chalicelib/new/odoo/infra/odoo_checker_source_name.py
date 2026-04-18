from dataclasses import dataclass
from typing import Any

from chalice import NotFoundError

from chalicelib.new.odoo.infra import OdooConnection


@dataclass
class OdooCheckerSourceName(OdooConnection):
    def get_source_id_by_name(self, source_name: str) -> Any:
        if not source_name or not self.need_odoo:
            return None
        cr = self.get_cr()
        if source_ids := cr.env["utm.source"].search([("name", "=", source_name)], limit=1):
            return source_ids[0]
        raise NotFoundError(f"Source name '{source_name}' does not exist")
