"""Permission routes — search and bulk permission assignment.

Ported from: backend/chalicelib/blueprints/permission.py
2 routes total.
"""

from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers.permission import PermissionController
from chalicelib.controllers.user import UserController
from chalicelib.schema.models.user import User
from dependencies import (
    common,
    get_current_user_rw,
    get_db_session,
    get_db_session_rw,
    get_json_body,
)

router = APIRouter(tags=["Permission"])


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    session: Session = Depends(get_db_session),
):
    return common.search(json_body, PermissionController, session=session)


@router.put("/")
def set_permissions(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    emails = body["emails"]
    permissions_by_company: dict[str, set[str]] = body["permissions"]
    context = {"user": user}
    result = UserController.set_permissions(
        emails, permissions_by_company, context=context, session=session
    )
    return {
        "state": "success",
        "users_processed": len(result),
        "companies_processed": len(permissions_by_company),
    }
