from datetime import datetime

import pytest
from sqlalchemy.orm import Session

from chalicelib.bus import get_global_bus
from chalicelib.new.cfdi_processor.domain.cfdi_exporter import CFDIExporter, ExportRepositoryS3
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import CFDIExportRepositorySA
from chalicelib.new.iva import IVAGetter
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.schema.models import CfdiExport as CfdiExportORM


@pytest.mark.skip(reason="This generates a persisting file")
def test_iva_export_ope_con_terceros(session: Session):
    company_identifier = (
        "e81a55ed-b9a0-4f27-8050-7b9274d74768"  # We pass UUID as company identifier
    )
    export_data = {
        "file_name": "test_iva_con_terceros",
        "type": "export-iva",
    }
    issued = DownloadType.ISSUED
    iva = "OpeConTer"
    period = datetime.fromisoformat("2025-05-01")

    export_request = CfdiExportORM(
        start=period.isoformat(),
        download_type=issued,
    )

    iva_exporter = CFDIExporter(
        session=session,
        cfdi_export_repo=CFDIExportRepositorySA(session=session),
        export_repo_s3=ExportRepositoryS3(),
        bus=get_global_bus(),
    )

    iva = IVAGetter(session=session)
    period_str = export_request.start
    period_date = datetime.fromisoformat(period_str).date()
    window_dates = iva.get_window_dates(period_date, False)

    query_ingresos, query_egresos = iva_exporter.get_combined_report(
        session=session,
        company_identifier=company_identifier,
        start_date=window_dates.period_start,
        end_date=window_dates.period_end,
    )
    iva_fields = {
        "RFC emisor": "RFC emisor",
        "Emisor": "Emisor",
        "Cantidad de CFDIs": "Cantidad de CFDIs",
        "Base IVA 16%": "Base IVA 16%",
        "Base IVA 8%": "Base IVA 8%",
        "Base IVA 0%": "Base IVA 0%",
        "Base IVA Exento": "Base IVA Exento",
        "IVA 16%": "IVA 16%",
        "IVA 8%": "IVA 8%",
        "Retenciones IVA": "Retenciones IVA",
    }

    egresos_rows = []

    headers = list(iva_fields.keys())
    egresos_rows.append(headers)

    for row in query_egresos:
        egresos_rows.append(
            [
                getattr(row, "RFC emisor", ""),
                getattr(row, "Emisor", ""),
                getattr(row, "Cantidad de CFDIs", 0),
                getattr(row, "Base IVA 16%", 0),
                getattr(row, "Base IVA 8%", 0),
                getattr(row, "Base IVA 0%", 0),
                getattr(row, "Base IVA Exento", 0),
                getattr(row, "IVA 16%", 0),
                getattr(row, "IVA 8%", 0),
                getattr(row, "Retenciones IVA", 0),
            ]
        )

    extra_pages = {"Egresos": egresos_rows}

    # Exportar con ambas hojas
    export_bytes = iva_exporter.export_iva_xlsx(
        query_ingresos, iva_fields, [], export_data, extra_pages
    )

    with open("test_iva_con_terceros.xlsx", "wb") as f:
        f.write(export_bytes)
