"""EFOS routes — blacklisted taxpayer monitoring.

Ported from: backend/chalicelib/blueprints/efos.py
3 routes total.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers import scale_to_super_user
from chalicelib.controllers.efos import EFOSController
from chalicelib.schema.models.user import User
from dependencies import (
    common,
    get_company_session,
    get_current_user,
    get_db_session_rw,
    get_json_body,
)

router = APIRouter(tags=["EFOS"])


@router.post("/update")
def update(
    session: Session = Depends(get_db_session_rw),
):
    context = scale_to_super_user()
    return EFOSController.update_from_sat(session=session, context=context)


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, EFOSController, session=company_session)


@router.post("/resume")
def resume(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
    user: User = Depends(get_current_user),
):
    return common.resume(json_body, EFOSController, session=company_session, user=user)
