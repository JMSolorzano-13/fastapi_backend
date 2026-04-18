import csv
import os
from dataclasses import dataclass
from datetime import date, datetime
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import IO

from sqlalchemy import func
from sqlalchemy.orm import Session

from chalicelib.logger import ERROR, EXCEPTION, log
from chalicelib.modules import Modules
from chalicelib.new.cfdi_processor.domain.cfdi_exporter import compress_dir_as_zip
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto import PastoTimeoutError
from chalicelib.new.pasto.exception import PastoInternalError
from chalicelib.new.pasto.paths import XMLZipPath
from chalicelib.new.pasto.request import PastoFileSender, PastoHTTPMethods
from chalicelib.new.query.infra.copy_query import copy_query
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models.tenant import CFDI as CFDIORM

csv.field_size_limit(envars.technical.CSV_FIELD_LIMIT)

SEND_RESULT = tuple[str, int, float, str]


@dataclass
class XMLSender(PastoFileSender):
    api_route: str
    endpoint: str
    session: Session

    action_code = "contpaqi-add-insert-xml-document-external"
    method = PastoHTTPMethods.POST
    request_api_route = "syncWorkerActions/add"

    def send_missing(
        self,
        request_identifier: Identifier,
        company_identifier: Identifier,
        pasto_company_identifier: Identifier,
        start: date,
        end: date,
        pasto_worker_token: str,
    ) -> SEND_RESULT:
        self.authorization = pasto_worker_token

        content, qty, amount = create_csv_cfdis_need_send_xml(self.session, start, end)
        if not qty:
            return "", 0, 0, ""
        path = XMLZipPath(company_identifier).path
        s3_url = self._upload_file(content, path)

        try:
            action = self._send_to_pasto(
                request_identifier=request_identifier,
                company_identifier=company_identifier,
                pasto_company_identifier=pasto_company_identifier,
                s3_url=s3_url,
            )
        except PastoTimeoutError:
            log(
                Modules.ADD_XML,
                ERROR,
                "TIMEOUT",
                {"request_identifier": request_identifier},
            )
            action = "ERROR"
        except PastoInternalError:
            log(
                Modules.ADD_XML,
                ERROR,
                "PASTO_ERROR",
                {"request_identifier": request_identifier},
            )
            action = "ERROR"
        except Exception as e:
            log(
                Modules.ADD_XML,
                EXCEPTION,
                "FAILED",
                {
                    "request_identifier": request_identifier,
                    "exception": e,
                },
            )
            action = "ERROR"

        return action, qty, amount, s3_url


def create_zip_and_csv(temp_file: IO[bytes]) -> tuple[bytes, int, float]:
    amount = 0
    qty = 0
    uuid_ix = 0
    xml_ix = 1
    total_ix = 2
    with TemporaryDirectory() as temp_dir, open(temp_file.name) as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            with open(os.path.join(temp_dir, f"{row[uuid_ix]}.xml"), "w", encoding="utf-8") as f:
                f.write(row[xml_ix])
            qty += 1
            amount += float(row[total_ix])
        return (
            compress_dir_as_zip(temp_dir),
            qty,
            amount,
        )


def create_csv_cfdis_need_send_xml(
    company_session: Session, start: date, end: date
) -> tuple[bytes, int, float]:
    # Build query using SQLAlchemy
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end, datetime.max.time())

    query = company_session.query(
        CFDIORM.UUID,
        CFDIORM.xml_content,
        func.coalesce(CFDIORM.TotalMXN, 0).label("TotalMXN"),
    ).filter(
        CFDIORM.Fecha >= start_dt,
        CFDIORM.Fecha <= end_dt,
        CFDIORM.from_xml,
        ~CFDIORM.add_exists,
    )
    query_str = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
    with NamedTemporaryFile("wb", suffix=".csv") as temp_file:
        copy_query(company_session, query_str, temp_file)
        temp_file.flush()
        return create_zip_and_csv(temp_file)
