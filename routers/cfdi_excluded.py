"""CFDI Excluded routes — excluded CFDIs search.

Ported from: backend/chalicelib/blueprints/cfdi_excluded.py
1 route total.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi_excluded import ExcludedCFDIController
from dependencies import common, get_company_session, get_json_body

router = APIRouter(tags=["CFDIExcluded"])


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, ExcludedCFDIController, session=company_session)
