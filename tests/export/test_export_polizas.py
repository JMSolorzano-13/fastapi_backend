import io
import random
from datetime import datetime, timedelta
from pathlib import Path

from openpyxl import load_workbook
from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.controllers.enums import ResumeType
from chalicelib.new.cfdi_processor.domain.cfdi_exporter import CFDIExporter
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import (
    CFDIExportRepositorySA,
)
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.poliza import Poliza
from chalicelib.schema.models.tenant.poliza_cfdi import PolizaCFDI


def random_datetime(start, end):
    delta = end - start
    segundos = delta.total_seconds()
    segundos_aleatorios = random.randrange(int(segundos))
    return start + timedelta(seconds=segundos_aleatorios)


def test_export_polizas(company_session: Session, tmp_path: Path):
    cfdis = [CFDI.demo() for _ in range(3)]
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
                relaciones=[PolizaCFDI(uuid_related=cfdis[0].UUID)],
            )
            for _ in range(2)
        ]
        + [
            Poliza(
                fecha=random_datetime(
                    start=datetime.fromisoformat("2020-01-01"),
                    end=datetime.fromisoformat("2024-01-01"),
                ),
                tipo=random.choice(["A", "B", "C"]),
                numero="".join(random.choices("0123456789", k=10)),
                relaciones=[PolizaCFDI(uuid_related=cfdis[1].UUID)],
            ),
        ]
    )

    company_session.commit()

    body = {
        "domain": [
            ["TipoDeComprobante", "=", "I"],
        ],
        "fuzzy_search": "",
        "limit": None,
        "offset": None,
        "order_by": None,
    }
    export_data = dict(
        {
            "file_name": "CFDI_poliza",
            "type": "",
        }
    )

    fields = [
        "UUID",
        "polizas_list",
    ]

    query = CFDIController._search(**body, session=company_session, fields=fields)

    exporter = CFDIExporter(
        company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
    )
    xlsx_bytes = exporter.export_xlsxv2(
        body=body,
        query=query,
        fields=fields,
        resume_type=ResumeType.BASIC,
        export_data=export_data,
        context=None,
    )

    wb = load_workbook(io.BytesIO(xlsx_bytes), data_only=True)

    wb_sheet = wb["CFDI"]

    # Sabemos que sólo recibiremos 2 columnas y 3 registros
    values_from_sheet = {
        wb_sheet["A2"].value: len(wb_sheet["B2"].value.split(",")) if wb_sheet["B2"].value else 0,
        wb_sheet["A3"].value: len(wb_sheet["B3"].value.split(",")) if wb_sheet["B3"].value else 0,
        wb_sheet["A4"].value: len(wb_sheet["B4"].value.split(",")) if wb_sheet["B4"].value else 0,
    }

    cfdis_db = company_session.query(CFDI).all()

    for cfdi in cfdis_db:
        poliza_qty = len(cfdi.polizas)

        # Validamos que cada UUID tiene la misma cantidad de polizas relacionadas
        assert values_from_sheet[cfdi.UUID] == poliza_qty

    # Descomentar esto si se quiere ver el excel
    # with open("cfdi_poliza.xlsx", "wb") as f:
    #     f.write(xlsx_bytes)
