import asyncio
import contextvars
import functools
from dataclasses import dataclass
from logging import DEBUG

from chalicelib.logger import EXCEPTION, INFO, log
from chalicelib.modules import Modules
from chalicelib.mx_edi import connectors
from chalicelib.mx_edi.connectors.sat.package import Package as WSPackage
from chalicelib.mx_edi.connectors.sat.package import (
    TooMuchDownloadsError as WSTooMuchDownloadsError,
)
from chalicelib.mx_edi.connectors.sat.sat_connector import SATConnector
from chalicelib.new.config.infra import envars
from chalicelib.new.package.domain.package import Package
from chalicelib.new.package.domain.package_repository import PackageRepository
from chalicelib.new.query.domain import Query
from chalicelib.new.query.domain.enums import QueryState
from chalicelib.new.shared.domain.event import EventType
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.infra.message.sqs_company import SQSUpdaterQuery
from chalicelib.new.utils.datetime import utc_now
from chalicelib.new.ws_sat.infra.ws import WSRepo


async def to_thread(func, /, *args, **kwargs):
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


@dataclass
class QueryDownloaderWS(WSRepo):
    bus: EventBus
    package_repo: PackageRepository
    connector: SATConnector | None = None

    def download(self, query: Query):
        connectors.sat.utils.REQUEST_TIMEOUT = envars.control.MAX_SAT_WS_DOWNLOAD_TIMEOUT
        query_body_j = query.model_dump_json()
        try:
            self.download_packages_and_save(query)
            log(
                Modules.SAT_WS_DOWNLOAD,
                DEBUG,
                "PACKAGE_DOWNLOADED",
                {
                    "query_execute_at": query.execute_at,
                    "query_identifier": query.identifier,
                    "company_identifier": query.company_identifier,
                    "body": query_body_j,
                },
            )
        except Exception as e:
            log(
                Modules.SAT_WS_DOWNLOAD,
                EXCEPTION,
                "PACKAGE_DOWNLOAD_FAILED",
                {
                    "query_execute_at": query.execute_at,
                    "query_identifier": query.identifier,
                    "company_identifier": query.company_identifier,
                    "body": query_body_j,
                    "exception": e,
                },
            )
            request = SQSUpdaterQuery(
                state=QueryState.ERROR,
                request_type=query.request_type,
                query_identifier=query.identifier,
                company_identifier=query.company_identifier,
                packages=query.packages or (),
                cfdis_qty=query.cfdis_qty,
                state_update_at=utc_now(),
            )
            self.bus.publish(
                EventType.WS_UPDATER,
                request,
            )
            return
        request = SQSUpdaterQuery(
            state=QueryState.DOWNLOADED,
            request_type=query.request_type,
            query_identifier=query.identifier,
            company_identifier=query.company_identifier,
            packages=query.packages or (),
            cfdis_qty=query.cfdis_qty,
            state_update_at=utc_now(),
        )
        self.bus.publish(
            EventType.WS_UPDATER,
            request,
        )
        self.bus.publish(
            EventType.SAT_WS_QUERY_DOWNLOADED,
            query,
        )

    async def async_download_package_and_save(self, package_id):
        return await to_thread(self.download_package_and_save, package_id)

    async def async_download_all_packages(self, query: Query):
        await asyncio.gather(
            *[self.async_download_package_and_save(package_id) for package_id in query.packages]
        )

    def mock_download(self, query: Query):
        for package_id in query.packages:
            self.package_repo.copy_mock(package_id)

    def download_packages_and_save(self, query: Query):
        if query.is_mocked:
            return self.mock_download(query)
        self.connector = self.get_sat_connector(wid=query.wid, cid=query.cid)
        log(
            Modules.SAT_WS_DOWNLOAD,
            INFO,
            "CONNECTOR_SET",
            {
                "company_identifier": query.company_identifier,
                "query_identifier": query.identifier,
                "rfc": self.connector.rfc,
            },
        )
        asyncio.run(self.async_download_all_packages(query))

    def download_package(self, package_id: str) -> Package:
        ws_package = WSPackage(package_id)
        try:
            ws_package.download(self.connector, process=False)
        except WSTooMuchDownloadsError:
            raise  # TODO
        return Package(
            sat_uuid=ws_package.identifier,
            zip_content=ws_package.binary,
        )

    def save_package(self, package: Package) -> None:
        self.package_repo.save(package)

    def download_package_and_save(self, package_id: str) -> None:
        package = self.download_package(package_id)
        self.save_package(package)
