import uuid

from sqlalchemy.orm import Session

from chalicelib.blueprints.common import _search
from chalicelib.blueprints.poliza import _create_many
from chalicelib.controllers.poliza import PolizaController


def test_create_poliza(company_session: Session):
    polizas = [
        {
            "identifier": str(uuid.uuid4()),
            "fecha": "2024-01-01T00:00:00",
            "tipo": "INGRESO",
            "numero": "0001",
            "concepto": "Concepto de prueba 1",
            "sistema_origen": "Sistema A",
            "cfdi_uuids": [
                str(uuid.uuid4()),
                str(uuid.uuid4()),
            ],
            "movimientos": [
                {
                    "numerador": "1",
                    "cuenta_contable": "1000",
                    "nombre": "Cuenta de Prueba",
                    "cargo": 0.1,
                    "abono": "0.00",
                    "cargo_me": "0.00",
                    "abono_me": "0.00",
                    "concepto": "Concepto del movimiento",
                    "referencia": "Ref001",
                },
                {
                    "numerador": "2",
                    "cuenta_contable": "2000",
                    "nombre": "Otra Cuenta de Prueba",
                    "cargo": "0.00",
                    "abono": "100.00",
                    "cargo_me": "0.00",
                    "abono_me": "0.00",
                    "concepto": "Concepto del segundo movimiento",
                    "referencia": "Ref002",
                },
            ],
        },
    ]
    _create_many(
        company_session,
        {"polizas": polizas},
    )
    polizas_in_db = _search(
        {
            "filters": [],
            "fields": [
                "identifier",
                "fecha",
                "tipo",
                "numero",
                "concepto",
                "sistema_origen",
                "relaciones.uuid_related",
                "movimientos.numerador",
                "movimientos.cuenta_contable",
                "movimientos.nombre",
                "movimientos.cargo",
                "movimientos.abono",
                "movimientos.cargo_me",
                "movimientos.abono_me",
                "movimientos.concepto",
                "movimientos.referencia",
            ],
        },
        PolizaController,
        company_session,
    )["data"]
    assert len(polizas_in_db) == len(polizas)
    assert len(polizas_in_db[0]["relaciones"]) == len(polizas[0]["relaciones"])
    assert len(polizas_in_db[0]["movimientos"]) == len(polizas[0]["movimientos"])
