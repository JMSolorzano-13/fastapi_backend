import odoorpc  # type: ignore

from chalicelib.controllers.common import CommonController
from chalicelib.new.config.infra import envars
from chalicelib.new.odoo.infra.odoo_checker_source_name import OdooCheckerSourceName
from chalicelib.schema.models import Model, User, Workspace


class OdooController(CommonController):
    model = Model

    @staticmethod
    def connection() -> odoorpc.ODOO:
        protocol = "jsonrpc+ssl" if envars.ODOO_PORT == 443 else "jsonrpc"
        odoo = odoorpc.ODOO(envars.ODOO_URL, port=envars.ODOO_PORT, protocol=protocol)
        odoo.login(envars.ODOO_DB, envars.ODOO_USER, envars.ODOO_PASSWORD)
        return odoo

    @staticmethod
    def _get_partner_by_mail(mail: str, odoo):
        partner_id = odoo.env["res.partner"].search(
            [("email", "=", mail)],
            limit=1,
        )
        return partner_id[0] if partner_id else None

    @staticmethod
    def create_partner_from_user(user: User, odoo):
        source_id = OdooCheckerSourceName().get_source_id_by_name(user.source_name)
        return odoo.env["res.partner"].create(
            {
                "name": user.name,
                "email": user.email,
                "source_id": source_id,
                "phone": user.phone,
            }
        )

    @staticmethod
    def ensure_partner(user: User, odoo) -> int:
        return OdooController._get_partner_by_mail(
            user.email, odoo
        ) or OdooController.create_partner_from_user(user, odoo)

    @staticmethod
    def new_workspace(workspace: Workspace, *, session):
        odoo = OdooController.connection()
        partner_id = OdooController.ensure_partner(workspace.owner, odoo)
        workspace.owner.odoo_identifier = partner_id
