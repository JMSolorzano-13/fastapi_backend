"""Param routes — system parameters search.

Ported from: backend/chalicelib/blueprints/param.py
1 route total.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers.param import ParamController
from dependencies import common, get_db_session, get_json_body

router = APIRouter(tags=["Param"])


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    session: Session = Depends(get_db_session),
):
    return common.search(json_body, ParamController, session=session)
