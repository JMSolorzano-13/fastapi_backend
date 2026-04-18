from dataclasses import dataclass

import odoorpc

from chalicelib.new.config.infra import envars


@dataclass
class OdooConnection:
    url: str = envars.ODOO_URL
    port: str = envars.ODOO_PORT
    db: str = envars.ODOO_DB
    user: str = envars.ODOO_USER
    password: str = envars.ODOO_PASSWORD
    need_odoo: bool = envars.NOTIFY_ODOO

    _cr: odoorpc.ODOO = None

    def get_cr(self) -> odoorpc.ODOO:
        if not self._cr:
            self._cr = self._new_connection()
        return self._cr

    def _new_connection(self) -> odoorpc.ODOO:
        protocol = "jsonrpc+ssl" if self.port == 443 else "jsonrpc"
        odoo = odoorpc.ODOO(self.url, port=self.port, protocol=protocol)
        odoo.login(self.db, self.user, self.password)
        return odoo
