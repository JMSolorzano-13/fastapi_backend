from chalice import CORSConfig, UnauthorizedError
from sqlalchemy.orm import Session

from chalicelib.config import PAGE_SIZE
from chalicelib.controllers.common import CommonController
from chalicelib.controllers.enums import ResumeType
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.schema.models.user import User

cors_config = CORSConfig(
    allow_origin="*",
    allow_headers=["access_token"],
    max_age=None,
    expose_headers=None,
    allow_credentials=None,
)


def get_user_token_from_request(bp) -> str:
    if token := bp.current_request.headers.get("access_token"):
        return token
    else:
        raise UnauthorizedError("No token provided")


def get_search_attrs(json_body):
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


special_export_types = {
    ResumeType.N,
    ResumeType.P,
}


def export(bp, controller: type[CommonController], company_session: Session, user: User):
    json_body = bp.current_request.json_body or {}
    export_data = json_body["export_data"]
    search_attrs = get_search_attrs(json_body)
    search_attrs["limit"] = None
    search_attrs["offset"] = None
    resume_type = json_body.get("TipoDeComprobante", ResumeType.BASIC.name)
    fields = search_attrs["fields"]
    export_format = json_body.get("format", "csv")
    resume_export = None
    resume_type = ResumeType[resume_type]
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
    if export_format in ["xlsx", "XLSX"]:
        if resume_type in special_export_types:
            resume_export = resume(
                bp, controller, session=company_session, user=user, resume_type=resume_type
            )
        else:
            resume_export = resume(bp, controller, session=company_session, user=user)
    elif not fields:
        fields = list(
            {"UUID", "xml_content"}
        )  # FIX: hack para forzar exportar UUID y xml_content en exportaciones de XML
        search_attrs["fields"] = fields
    query = controller._search(  # pylint: disable=protected-access
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


def massive_export(bp, controller: type[CommonController], session: Session):
    json_body = bp.current_request.json_body or {}

    search_attrs = get_search_attrs(json_body)
    search_attrs["limit"] = None
    search_attrs["offset"] = None
    json_body.get("fields", [])
    json_body.get("format", "csv")

    return json_body


def search(bp, controller: type[CommonController], session: Session):
    json_body = bp.current_request.json_body or {}
    return _search(json_body, controller, session)


def _search(json_body: dict, controller: type[CommonController], session: Session) -> dict:
    search_attrs = get_search_attrs(json_body)

    log(
        Modules.SEARCH,
        DEBUG,
        "SEARCH",
        {
            "endpoint": "CFDI/search",
            "body": json_body,
        },
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


def create(bp, controller: type[CommonController], session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    context = {"user": user}
    po = controller.create(
        json_body, context=context, session=session
    )  # TODO use `data` section and allow list of values
    dict_repr = controller.to_nested_dict(po)
    return dict_repr[0]


def update(bp, controller: type[CommonController], session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    ids = set(json_body["ids"])
    values = json_body["values"]
    context = {"user": user}
    pos = controller.get(ids, context=context, session=session)
    controller.update(pos, values, context=context, session=session)
    return controller.to_nested_dict(pos)


def delete(bp, controller: type[CommonController], session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    context = {"user": user}
    ids = set(json_body["ids"])
    pos = controller.get(ids, context=context, session=session)
    ids = controller.delete(pos, context=context, session=session)
    return {"deleted": list(ids)}


def resume(
    bp,
    controller: type[CommonController],
    session: Session,
    user: User,
    resume_type=ResumeType.BASIC,
):
    json_body = bp.current_request.json_body or {}

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
