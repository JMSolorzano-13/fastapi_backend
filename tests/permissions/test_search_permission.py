import types

from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.controllers.permission import PermissionController


def test_search_permission(session: Session, company):
    json_body = {
        "domain": [
            ["company_id", "in", [company.id]],
        ],
        "fields": ["role", "company.name", "user.name", "user.email", "user.source_name"],
    }

    request_mock = types.SimpleNamespace(json_body=json_body)
    bp_mock = types.SimpleNamespace(current_request=request_mock)

    result = common.search(bp_mock, PermissionController, session)

    assert result["data"]

    assert result["data"][0]["role"] in ["OPERATOR", "PAYROLL"]
