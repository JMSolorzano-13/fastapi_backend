"""Regimen Fiscal routes — fiscal regime retrieval from Odoo.

Ported from: backend/chalicelib/blueprints/regimen_fiscal.py
1 route total.
"""

from fastapi import APIRouter

from chalicelib.new.odoo import OdooConnection
from chalicelib.new.regimen_fiscal import RegimenFiscalRetriever

router = APIRouter(tags=["RegimenFiscal"])


@router.get("", include_in_schema=False)
@router.get("/")
def get_all():
    connection = OdooConnection()
    return RegimenFiscalRetriever(connection).get_all()
