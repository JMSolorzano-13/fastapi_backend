import random
import uuid
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from chalicelib.blueprints.poliza import _create_many
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.poliza import Poliza
from chalicelib.schema.models.tenant.poliza_cfdi import PolizaCFDI


def random_datetime(start, end):
    delta = end - start
    segundos = delta.total_seconds()
    segundos_aleatorios = random.randrange(int(segundos))
    return start + timedelta(seconds=segundos_aleatorios)


def test_poliza(company_session: Session, company):
    random.seed(42)
    cfdis = [CFDI.demo() for _ in range(100)]
    company_session.add_all(cfdis)
    company_session.add_all(
        [
            Poliza(
                fecha=random_datetime(
                    start=datetime.fromisoformat("2020-01-01"),
                    end=datetime.fromisoformat("2024-01-01"),
                ),
                tipo=random.choice(["A", "B", "C"]),
                numero="".join(random.choices("0123456789", k=10)),
                relaciones=[
                    PolizaCFDI(uuid_related=random.choice([cfdi.UUID, str(uuid.uuid4())]))
                    for cfdi in random.sample(cfdis, k=random.randint(0, 3))
                ],
            )
            for _ in range(50)
        ]
        + [
            Poliza(
                fecha=random_datetime(
                    start=datetime.fromisoformat("2020-01-01"),
                    end=datetime.fromisoformat("2024-01-01"),
                ),
                tipo=random.choice(["A", "B", "C"]),
                numero="".join(random.choices("0123456789", k=10)),
                relaciones=[
                    PolizaCFDI(uuid_related=cfdis[0].UUID)  # Forzar relación con el primer CFDI
                ],
            ),
            Poliza(
                fecha=random_datetime(
                    start=datetime.fromisoformat("2020-01-01"),
                    end=datetime.fromisoformat("2024-01-01"),
                ),
                tipo=random.choice(["A", "B", "C"]),
                numero="".join(random.choices("0123456789", k=10)),
                relaciones=[
                    PolizaCFDI(uuid_related=cfdis[0].UUID)  # Forzar relación con el primer CFDI
                ],
            ),
        ]
    )
    company_session.commit()
    polizas = company_session.query(Poliza).all()
    for poliza in polizas:
        print(f"Poliza {poliza.identifier} - {poliza.fecha} - {poliza.tipo} - {poliza.numero}")
        for relacion in poliza.relaciones:
            print(
                f"  CFDI relacionado: {relacion.uuid_related}, Tipo: {relacion.cfdi_related.TipoDeComprobante if relacion.cfdi_related else 'N/A'}"
            )
        for cfdi in poliza.cfdis:
            print(f"  CFDI (via M:N): {cfdi.UUID}, Tipo: {cfdi.TipoDeComprobante}")


def test_delete_if_no_complete_data(company_session: Session):
    poliza_incomplete_1 = {
        "identifier": str(uuid.uuid4()),
        # Missing 'fecha', 'tipo', 'numero'
    }

    poliza_incomplete_2 = {
        "identifier": str(uuid.uuid4()),
        "fecha": "2023-01-01",
        # Missing 'tipo', 'numero'
    }
    poliza_complete_1 = {
        "identifier": str(uuid.uuid4()),
        "fecha": "2023-01-01",
        "tipo": "A",
        "numero": "1234567890",
    }
    _create_many(
        company_session,
        {
            "polizas": [
                poliza_incomplete_1,
                poliza_incomplete_2,
                poliza_complete_1,
            ]
        },
    )
    assert (
        company_session.query(Poliza)
        .filter(Poliza.identifier == poliza_incomplete_1["identifier"])
        .first()
        is None
    )
    assert (
        company_session.query(Poliza)
        .filter(Poliza.identifier == poliza_incomplete_2["identifier"])
        .first()
        is None
    )
    assert (
        company_session.query(Poliza)
        .filter(Poliza.identifier == poliza_complete_1["identifier"])
        .first()
        is not None
    )

    # Test delete existing complete poliza using incomplete data
    poliza_to_delete = poliza_complete_1
    poliza_to_delete.pop("fecha")
    _create_many(
        company_session,
        {
            "polizas": [
                poliza_to_delete,
            ]
        },
    )
    assert (
        company_session.query(Poliza)
        .filter(Poliza.identifier == poliza_to_delete["identifier"])
        .first()
        is None
    )
