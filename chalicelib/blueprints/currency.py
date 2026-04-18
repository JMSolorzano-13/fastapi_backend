from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.currency import CurrencyController

bp = SuperBlueprint(__name__)


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search():
    json_body = bp.current_request.json_body or {}

    search_attrs = common.get_search_attrs(json_body)

    pos, next_page, total_records = CurrencyController.search(**search_attrs)
    dict_repr = CurrencyController.detail(pos)
    return {
        "data": dict_repr,
        "next_page": next_page,
        "total_records": total_records,
    }
