from dataclasses import dataclass

from chalicelib.new.fiscal_data.domain import FiscalData
from chalicelib.new.odoo import OdooConnection
from chalicelib.new.odoo.config import get_regimen_fiscal_field


class FiscalDataUpdateError(Exception):
    pass


@dataclass
class FiscalDataUpdater:
    connection: OdooConnection

    def get_partner(self, cr, odoo_identifier) -> "res.partner":  # noqa E501
        exception = FiscalDataUpdateError(
            "Error retrieving odoo partner, "
            "maybe the user is no linked to an odoo partner. "
            f"Partner id searched: {odoo_identifier}",
        )
        try:
            partner = cr.env["res.partner"].browse(odoo_identifier)
        except Exception as e:
            raise exception from e
        if not partner:
            raise exception
        return partner

    def retrieve(self, odoo_identifier: int) -> FiscalData:
        cr = self.connection.get_cr()
        partner = self.get_partner(cr, odoo_identifier)
        regimen_fiscal = getattr(partner, get_regimen_fiscal_field())
        regimen_fiscal_id = regimen_fiscal and regimen_fiscal.id or False
        return FiscalData(
            regimen_fiscal_id=regimen_fiscal_id,
            nombre=partner.name,
            rfc=partner.vat,
            cp=partner.zip,
            email=partner.email,
        )

    def update(self, odoo_identifier: int, data: FiscalData) -> None:
        cr = self.connection.get_cr()
        partner = self.get_partner(cr, odoo_identifier)
        partner.write(  # TODO use invoice contact
            {
                get_regimen_fiscal_field(): data.regimen_fiscal_id,
                "name": data.nombre,
                "vat": data.rfc,
                "zip": data.cp,
                "email": data.email,
            }
        )
