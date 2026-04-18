"""Poliza routes — accounting policy management.

Ported from: backend/chalicelib/blueprints/poliza.py
3 routes total.
"""

from fastapi import APIRouter, Body, Depends
from sqlalchemy import tuple_
from sqlalchemy.orm import Session

from chalicelib.controllers.poliza import PolizaController
from chalicelib.exceptions import DocDefaultException
from chalicelib.schema.models.tenant.poliza import Poliza
from chalicelib.schema.models.tenant.poliza_cfdi import PolizaCFDI
from chalicelib.schema.models.tenant.poliza_movimiento import PolizaMovimiento
from chalicelib.schema.models.user import User
from dependencies import (
    common,
    get_company_session,
    get_company_session_rw,
    get_current_user,
    get_json_body,
)
from exceptions import BadRequestError

router = APIRouter(tags=["Poliza"])


class DuplicatePolizaError(DocDefaultException, BadRequestError):
    pass


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, PolizaController, session=company_session)


@router.post("/create_many")
def create_many(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
):
    return _create_many(company_session, body)


@router.post("/export")
def export(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session_rw),
    user: User = Depends(get_current_user),
):
    return common.export(json_body, PolizaController, company_session=company_session, user=user)


def _create_many(company_session: Session, body: dict) -> dict:
    polizas: list[Poliza] = []
    to_delete: list[str] = []
    pseudo_pk: list[tuple[str, str, str]] = []
    polizas_json: list[dict] = body["polizas"]
    for poliza_json in polizas_json:
        poliza_json["relaciones"] = [
            PolizaCFDI(uuid_related=uuid) for uuid in poliza_json.pop("cfdi_uuids", [])
        ]

        poliza_json["movimientos"] = [
            PolizaMovimiento.from_dict(movimiento)
            for movimiento in poliza_json.pop("movimientos", [])
        ]

        poliza = Poliza(**poliza_json)
        if poliza.fecha and poliza.tipo and poliza.numero:
            polizas.append(poliza)
            pseudo_pk.append((poliza.fecha, poliza.tipo, poliza.numero))

        to_delete.append(poliza.identifier)

    company_session.query(Poliza).filter(Poliza.identifier.in_(to_delete)).delete(
        synchronize_session=False
    )
    already_existing = (
        company_session.query(Poliza)
        .filter(tuple_(Poliza.fecha, Poliza.tipo, Poliza.numero).in_(pseudo_pk))
        .all()
    )
    if already_existing:
        existing_str = ",\n".join(
            f"(`{p.identifier}`, `{p.fecha}`, `{p.tipo}`, `{p.numero}`)" for p in already_existing
        )
        raise DuplicatePolizaError(
            f"Ya existen las siguientes pólizas (identifier, fecha, tipo, número):\n{existing_str}"
        )
    company_session.add_all(polizas)
    return {"state": "success", "created": len(polizas)}
