from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.new.odoo import OdooConnection
from chalicelib.new.regimen_fiscal import RegimenFiscalRetriever

bp = SuperBlueprint(__name__)


@bp.route("/", methods=["GET"], cors=common.cors_config)
def get_all():
    connection = OdooConnection()
    return RegimenFiscalRetriever(connection).get_all()
