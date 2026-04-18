import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import StringIO
from tempfile import NamedTemporaryFile

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from chalicelib.logger import ERROR, EXCEPTION, log
from chalicelib.modules import Modules
from chalicelib.new.pasto.exception import PastoInternalError, PastoTimeoutError
from chalicelib.new.pasto.paths import CancelPath
from chalicelib.new.pasto.request import (
    PastoFileSender,
    PastoFileSenderResult,
    PastoHTTPMethods,
)
from chalicelib.new.query.infra.copy_query import copy_query
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models.tenant import CFDI as CFDIORM

CSV_URL = str

COLUMN_TO_SYNC = "Fecha"


@dataclass
class Canceler(PastoFileSender):
    endpoint: str
    api_route: str
    session: Session
    bucket: str
    expires_in: timedelta
    method = PastoHTTPMethods.POST
    action_code = "contpaqi-add-update-document-estatus-external"
    request_api_route = "syncWorkerActions/add"

    def cancel_missing(
        self,
        request_identifier: Identifier,
        company_identifier: Identifier,
        pasto_company_identifier: Identifier,
        pasto_worker_token: str,
        start: date,
        end: date,
    ) -> PastoFileSenderResult:
        self.authorization = pasto_worker_token

        content, qty, amount = create_csv_cfdis_needs_to_be_cancelled(self.session, start, end)
        if not qty:
            return "", 0, 0, ""

        path = CancelPath(company_identifier).path
        s3_url = self._upload_file(content, path)

        context = (
            {
                "company_identifier": company_identifier,
                "request_id": request_identifier,
            },
        )
        try:
            action = self._send_to_pasto(
                request_identifier=request_identifier,
                company_identifier=company_identifier,
                pasto_company_identifier=pasto_company_identifier,
                s3_url=s3_url,
            )
        except PastoTimeoutError:
            log(
                Modules.ADD_CANCEL,
                ERROR,
                "TIMEOUT",
                context,
            )
            action = "ERROR"
        except PastoInternalError:
            log(
                Modules.ADD_CANCEL,
                ERROR,
                "PASTO_ERROR",
                context,
            )
            action = "ERROR"
        except Exception as e:
            log(
                Modules.ADD_CANCEL,
                EXCEPTION,
                "FAILED",
                context=context | {"exception": e},
            )
            action = "ERROR"

        return (action, qty, amount, s3_url)


def create_csv_cfdis_needs_to_be_cancelled(
    company_session: Session, start: date, end: date
) -> tuple[str, int, float]:
    # Build query using SQLAlchemy
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end, datetime.max.time())

    query = company_session.query(
        CFDIORM.UUID,
        CFDIORM.FechaCancelacion,
        func.coalesce(CFDIORM.TotalMXN, 0).label("TotalMXN"),
    ).filter(
        CFDIORM.Fecha >= start_dt,
        CFDIORM.Fecha <= end_dt,
        ~CFDIORM.Estatus,
        or_(
            CFDIORM.add_cancel_date.is_(None),
            CFDIORM.FechaCancelacion != CFDIORM.add_cancel_date,
        ),
        or_(CFDIORM.from_xml, CFDIORM.add_exists),
    )

    query_str = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
    with NamedTemporaryFile("wb", suffix=".csv") as temp_file:
        copy_query(company_session, query_str, temp_file)
        temp_file.flush()
        with open(temp_file.name, encoding="UTF-8") as csv_file, StringIO() as content:
            reader = csv.reader(csv_file)
            writer = csv.writer(content)
            total = 0
            qty = 0
            total_ix = 2  # Ensure that the index is correct and is at the end of the row
            writer.writerow(["uuid", "cancel_date"])  # Header
            next(reader, None)
            for row in reader:
                qty += 1
                total += float(row[total_ix])
                writer.writerow(row[:total_ix])
            content.seek(0)
            return content.read(), qty, total
