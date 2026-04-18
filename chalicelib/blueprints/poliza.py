from chalice import BadRequestError
from sqlalchemy import tuple_
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.poliza import PolizaController
from chalicelib.exceptions import DocDefaultException
from chalicelib.schema.models.tenant.poliza import Poliza
from chalicelib.schema.models.tenant.poliza_cfdi import PolizaCFDI
from chalicelib.schema.models.tenant.poliza_movimiento import PolizaMovimiento
from chalicelib.schema.models.user import User

bp = SuperBlueprint(__name__)


class DuplicatePolizaError(DocDefaultException, BadRequestError):
    pass


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(company_session: Session):
    return common.search(bp, PolizaController, session=company_session)


@bp.route("/create_many", methods=["POST"], cors=common.cors_config, read_only=False)
def create_many(company_session: Session):
    return _create_many(company_session, bp.current_request.json_body)


@bp.route("/export", methods=["POST"], cors=common.cors_config, read_only=False)
def export(company_session: Session, user: User):
    return common.export(bp, PolizaController, company_session=company_session, user=user)


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
        if poliza.fecha and poliza.tipo and poliza.numero:  # Solo si trae todas sus PK
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
