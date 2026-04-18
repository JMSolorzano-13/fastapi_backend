"""CRUD helper functions — ported from chalicelib/blueprints/common.py.

Refactored: ``bp`` first-argument replaced with an explicit ``json_body: dict``
parameter so there is no Chalice dependency.
"""

from sqlalchemy.orm import Session

from chalicelib.config import PAGE_SIZE
from chalicelib.controllers.common import CommonController
from chalicelib.controllers.enums import ResumeType
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.schema.models.user import User
from exceptions import BadRequestError

special_export_types = {
    ResumeType.N,
    ResumeType.P,
}


def get_search_attrs(json_body: dict) -> dict:
    attr_list = {
        "fuzzy_search": None,
        "fields": [],
        "domain": {},
        "order_by": None,
        "limit": PAGE_SIZE,
        "offset": None,
        "active": True,
    }
    return {attr: json_body.get(attr, default) for attr, default in attr_list.items()}


def search(json_body: dict, controller: type[CommonController], session: Session) -> dict:
    return _search(json_body, controller, session)


def _search(json_body: dict, controller: type[CommonController], session: Session) -> dict:
    search_attrs = get_search_attrs(json_body)
    log(
        Modules.SEARCH,
        DEBUG,
        "SEARCH",
        {"endpoint": "CFDI/search", "body": json_body},
    )
    records, next_page, total_records = controller.search(
        **search_attrs, context={}, session=session
    )
    dict_repr = controller.to_nested_dict(records)
    return {
        "data": dict_repr,
        "next_page": next_page,
        "total_records": total_records,
    }


def create(
    json_body: dict,
    controller: type[CommonController],
    session: Session,
    user: User,
):
    context = {"user": user}
    po = controller.create(json_body, context=context, session=session)
    dict_repr = controller.to_nested_dict(po)
    return dict_repr[0]


def update(
    json_body: dict,
    controller: type[CommonController],
    session: Session,
    user: User,
):
    if "ids" not in json_body:
        raise BadRequestError("ids is required")
    if "values" not in json_body:
        raise BadRequestError("values is required")
    ids = set(json_body["ids"])
    values = json_body["values"]
    context = {"user": user}
    pos = controller.get(ids, context=context, session=session)
    controller.update(pos, values, context=context, session=session)
    return controller.to_nested_dict(pos)


def delete(
    json_body: dict,
    controller: type[CommonController],
    session: Session,
    user: User,
):
    context = {"user": user}
    if "ids" not in json_body:
        raise BadRequestError("ids is required")
    ids = set(json_body["ids"])
    pos = controller.get(ids, context=context, session=session)
    ids = controller.delete(pos, context=context, session=session)
    return {"deleted": list(ids)}


def resume(
    json_body: dict,
    controller: type[CommonController],
    session: Session,
    user: User,
    resume_type: ResumeType = ResumeType.BASIC,
):
    domain = json_body.get("domain", [])
    fuzzy_search = json_body.get("fuzzy_search", [])
    fields = json_body.get("fields", [])
    context = {"user": user}
    return controller.resume(
        domain,
        fuzzy_search,
        context=context,
        resume_type=resume_type,
        session=session,
        fields=fields,
    )


def export(
    json_body: dict,
    controller: type[CommonController],
    company_session: Session,
    user: User,
):
    export_data = json_body["export_data"]
    search_attrs = get_search_attrs(json_body)
    search_attrs["limit"] = None
    search_attrs["offset"] = None
    resume_type_name = json_body.get("TipoDeComprobante", ResumeType.BASIC.name)
    fields = search_attrs["fields"]
    export_format = json_body.get("format", "csv")
    resume_export = None
    resume_type = ResumeType[resume_type_name]
    log(
        Modules.EXPORT,
        DEBUG,
        "EXPORT_INDIVIDUAL",
        {
            "company_identifier": json_body["domain"][0][2],
            "body": json_body,
            "resume_type": resume_type,
            "export_format": export_format,
        },
    )
    if export_format in ("xlsx", "XLSX"):
        if resume_type in special_export_types:
            resume_export = resume(
                json_body, controller, session=company_session, user=user, resume_type=resume_type
            )
        else:
            resume_export = resume(json_body, controller, session=company_session, user=user)
    elif not fields:
        fields = list({"UUID", "xml_content"})
        search_attrs["fields"] = fields

    query = controller._search(
        **search_attrs,
        lazzy=True,
        session=company_session,
    )
    return controller.export(
        export_data,
        query,
        fields,
        export_format,
        resume_export,
        context={},
        resume_type=resume_type,
        session=company_session,
    )


def massive_export(
    json_body: dict,
    controller: type[CommonController],
    session: Session,
):
    search_attrs = get_search_attrs(json_body)
    search_attrs["limit"] = None
    search_attrs["offset"] = None
    json_body.get("fields", [])
    json_body.get("format", "csv")
    return json_body
