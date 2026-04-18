from dataclasses import dataclass

from chalicelib.new.odoo import OdooConnection
from chalicelib.new.odoo.config import get_regimen_fiscal_model


class RegimenFiscalRetrieverError(Exception):
    pass


RegimenFiscalRetrieverResult = dict[int, str]


@dataclass
class RegimenFiscalRetriever:
    connection: OdooConnection

    def get_all(self) -> RegimenFiscalRetrieverResult:
        cr = self.connection.get_cr()
        try:
            ids = cr.env[get_regimen_fiscal_model()].search([])
            data_dict = cr.env[get_regimen_fiscal_model()].read(ids, ["id", "name"])
            return {d["id"]: d["name"] for d in data_dict}
        except Exception as e:
            raise RegimenFiscalRetrieverError("Error retrieving fiscal data") from e
