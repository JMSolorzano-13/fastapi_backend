import csv
import io
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from chalicelib.boto3_clients import s3_client
from chalicelib.logger import DEBUG, Modules, log
from chalicelib.new.pasto import ADDSyncRequester
from chalicelib.new.pasto.paths import MetadataPath
from chalicelib.new.query.infra.copy_query import copy_query
from chalicelib.new.query.infra.temp_table_sa import update_multiple
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.datetime import utc_now
from chalicelib.schema.models import Company as CompanyORM
from chalicelib.schema.models.tenant import CFDI as CFDIORM

UUIDsState = Iterable[dict[str, Any]]


@dataclass
class MetadataUpdater:
    session: Session | None
    bucket: str | None
    bus: EventBus | None

    def update_exists(
        self,
        company_session: Session,
        new: set[Identifier] | None = None,
        missing: set[Identifier] | None = None,
    ):
        if new:
            update_multiple(
                dest_table=CFDIORM.get_specific_table(),
                key="UUID",
                field_types=[("UUID", "UUID")],
                records=({"UUID": uuid} for uuid in new),
                session=company_session,
                fields_to_update_hardcoded={"add_exists": True},
            )
        if missing:
            update_multiple(
                dest_table=CFDIORM.get_specific_table(),
                key="UUID",
                field_types=[("UUID", "UUID")],
                records=({"UUID": uuid} for uuid in missing),
                session=company_session,
                fields_to_update_hardcoded={"add_exists": False, "add_cancel_date": "NULL"},
            )

    def update_cancel_date_optimistic(
        self,
        to_cancel: set[Identifier],
        company_session: Session,
    ):
        update_multiple(
            dest_table=CFDIORM.get_specific_table(),
            key="UUID",
            field_types=[("UUID", "UUID")],
            records=({"UUID": uuid} for uuid in to_cancel),
            session=company_session,
            fields_to_update_same_table={"add_cancel_date": "FechaCancelacion"},
        )

    def update_cancel_date(
        self,
        company_session: Session,
        to_cancel: dict[Identifier, datetime] | None = None,
        to_reactivate: set[Identifier] | None = None,
    ):
        if to_cancel:
            update_multiple(
                dest_table=CFDIORM.get_specific_table(),
                key="UUID",
                field_types=[("UUID", "UUID"), ("add_cancel_date", "TIMESTAMP")],
                records=(
                    {"UUID": uuid, "add_cancel_date": cancel_date.isoformat()}
                    for uuid, cancel_date in to_cancel.items()
                ),
                session=company_session,
                fields_to_update=["add_cancel_date"],
            )
        if to_reactivate:
            update_multiple(
                dest_table=CFDIORM.get_specific_table(),
                key="UUID",
                field_types=[("UUID", "UUID")],
                records=({"UUID": uuid} for uuid in to_reactivate),
                session=company_session,
                fields_to_update_hardcoded={"add_cancel_date": "NULL"},
            )

    def get_data_from_csv(self, csv_reader):
        header: tuple[str] = next(csv_reader)
        uuid_ix, cancel_date_ix = (
            header.index("uuid"),
            header.index("cancel_date"),
        )
        exists_in_add = set()
        cancel_in_add = {}
        for row in csv_reader:
            if not row[uuid_ix]:  # Skip empty uuids
                continue
            exists_in_add.add(row[uuid_ix])
            if row[cancel_date_ix]:
                cancel_in_add[row[uuid_ix]] = datetime.fromisoformat(row[cancel_date_ix])
        return exists_in_add, cancel_in_add

    def _get_data_from_db(
        self, company_session: Session, start: datetime | None = None, end: datetime | None = None
    ):
        table = CFDIORM.get_specific_table()
        date_filter = ""
        if start:
            date_filter += f" AND \"Fecha\" >= '{start.isoformat()}'"
        if end:
            date_filter += f" AND \"Fecha\" <= '{end.isoformat()}'"
        query = f"""
            SELECT
                "UUID" AS "uuid",
                "add_cancel_date" AS "cancel_date"
            FROM "{table}"
            WHERE
                add_exists = TRUE
                {date_filter}
        """
        with io.StringIO() as file:
            copy_query(company_session, query, file)
            csv_reader = csv.reader(file.getvalue().splitlines())
            return self.get_data_from_csv(csv_reader)

    # TODO: Esta función se ejecuta cuando, se supone que se carga la data de la empresa vinculada.
    # TODO: Validar con una cuenta en prod de Plataforma
    def update(self, company_identifier: Identifier, company_session: Session):
        csv_reader = self._get_csv(MetadataPath(company_identifier).path)
        self._update_add_metadata(company_session, csv_reader)

        company: CompanyORM = (
            self.session.query(CompanyORM).filter(CompanyORM.identifier == company_identifier).one()
        )
        company.pasto_last_metadata_sync = utc_now()

        if not company.add_auto_sync:
            log(
                Modules.ADD_METADATA,
                DEBUG,
                "AUTO_SYNC_DISABLED",
                {
                    "company_identifier": company_identifier,
                },
            )
            return

        log(
            Modules.ADD_METADATA,
            DEBUG,
            "AUTO_SYNC_ENABLED",
            {
                "company_identifier": company_identifier,
            },
        )
        requester = ADDSyncRequester(company_session=company_session, bus=self.bus)
        requester.request(
            company_identifier=company_identifier,
            pasto_company_identifier=company.pasto_company_identifier,
            pasto_token=company.workspace.pasto_worker_token,
        )

    def _update_add_metadata(
        self,
        company_session: Session,
        csv_reader,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        exists_in_add, cancel_in_add = self.get_data_from_csv(csv_reader)
        exists_in_db, cancel_in_db = self._get_data_from_db(company_session, start=start, end=end)

        new, missing = exists_in_add - exists_in_db, exists_in_db - exists_in_add
        to_cancel, to_reactivate = (
            cancel_in_add.keys() - cancel_in_db.keys(),
            cancel_in_db.keys() - cancel_in_add.keys(),
        )
        to_cancel = {uuid: cancel_in_add[uuid] for uuid in to_cancel}

        self.update_exists(
            new=new,
            missing=missing,
            company_session=company_session,
        )
        self.update_cancel_date(
            company_session=company_session,
            to_cancel=to_cancel,
            to_reactivate=to_reactivate,
        )

    def set_pasto_last_metadata_sync_null(self, company_identifier: Identifier):
        self.session.query(CompanyORM).filter(
            CompanyORM.identifier == company_identifier,
        ).update(
            {
                CompanyORM.pasto_last_metadata_sync: None,
            }
        )

    def _get_csv(self, path: str):
        assert self.bucket
        return get_csv(
            bucket=self.bucket,
            key=path,
        )


def get_csv(bucket: str, key: str):
    csv_file = io.BytesIO()
    s3_client().download_fileobj(
        Fileobj=csv_file,
        Bucket=bucket,
        Key=key,
    )
    csv_file.seek(0)
    return csv.reader(csv_file.read().decode("utf-8").splitlines())
