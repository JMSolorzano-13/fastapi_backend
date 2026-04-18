"""CFDI Export routes — export record search.

Ported from: backend/chalicelib/blueprints/cfdi_export.py
1 route total.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi_export import CfdiExportController
from dependencies import common, get_company_session, get_json_body

router = APIRouter(tags=["Export"])


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, CfdiExportController, session=company_session)
