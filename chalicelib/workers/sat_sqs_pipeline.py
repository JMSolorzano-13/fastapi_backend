"""SAT + related SQS pipeline steps extracted from app.py for reuse without Chalice."""

import random
from datetime import datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import update
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import s3_client
from chalicelib.config import PAGE_SIZE
from chalicelib.bus import get_global_bus
from chalicelib.controllers.enums import ResumeType
from chalicelib.controllers.tenant.session import (
    new_company_session_from_company_identifier,
    with_company_session_from_message_reuse_connection,
)
from chalicelib.logger import DEBUG, EXCEPTION, INFO, WARNING, log
from chalicelib.modules import Modules
from chalicelib.new.cfdi_processor.domain.cfdi_exporter import (
    CFDIExporter,
    ExportRepositoryS3,
)
from chalicelib.new.cfdi_processor.domain.query_cfdis_completer import QueryCFDISCompleter
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import CFDIExportRepositorySA
from chalicelib.new.cfdi_processor.infra.messages.need_to_complete_cfdis import (
    NeedToCompleteCFDIsEvent,
)
from chalicelib.new.cfdi_processor.infra.messages.payload_message import SQSMessagePayload
from chalicelib.new.company.infra.company_repository_sa import CompanyRepositorySA
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.rds_verifier import RDSVerifier
from chalicelib.new.package.infra.package_repository_s3 import PackageRepositoryS3
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.query.domain.events.query_sent_event import QueryCreateEvent
from chalicelib.new.query.domain.metadata_processor import MetadataProcessor, MetadataRepositoryZip
from chalicelib.new.query.domain.query import Query
from chalicelib.new.query.domain.query_creator import QueryCreator, rfc_from_cid
from chalicelib.new.query.domain.xml_processor import XMLProcessor, XMLRepositoryZip
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.new.query.infra.query_repository_sa import QueryRepositorySA
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.infra.message.sqs_company import SQSCompanySendMetadata, SQSUpdaterQuery
from chalicelib.workers.verify_query_payload import normalize_verify_query_body
from chalicelib.new.shared.infra.sqs_handler import SQSHandler
from chalicelib.new.utils.datetime import utc_now
from chalicelib.new.ws_sat.domain.events import QueryNeedToBeSplittedEvent
from chalicelib.new.ws_sat.infra.query_downloader_ws import QueryDownloaderWS
from chalicelib.new.ws_sat.infra.query_sender_ws import QuerySenderWS
from chalicelib.new.ws_sat.infra.query_updater_ws import QueryUpdaterWS
from chalicelib.new.ws_sat.infra.query_verifier_ws import QueryVerifierWS
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import SATQuery as SATQueryORM
from chalicelib.workers.sqs_lambda_shim import dict_to_sqs_event_records, sqs_handle_events


def _export_search_attrs_from_json(json_body: dict) -> dict:
    """Same keys as chalicelib.blueprints.common.get_search_attrs (no Chalice blueprint import)."""
    attr_list = {
        "fuzzy_search": None,
        "fields": [],
        "domain": {},
        "order_by": None,
        "limit": PAGE_SIZE,
        "offset": None,
        "active": True,
    }
    return {attr: json_body.get(attr, default) for attr, default in attr_list.items()}


def process_sqs_create_query(events, session: Session) -> None:
    bus = get_global_bus()

    @with_company_session_from_message_reuse_connection(session=session, read_only=False)
    def _sqs_create_query(message: QueryCreateEvent, company_session: Session):
        query_repo = QueryRepositorySA(session=company_session)
        sender = QuerySenderWS(
            bus=bus,
            company_session=company_session,
            session=session,
        )
        creator = QueryCreator(query_repo=query_repo, session=session)
        query = creator.create(
            company_identifier=message.company_identifier,
            download_type=message.download_type,
            request_type=message.request_type,
            is_manual=message.is_manual,
            start=message.start,
            end=message.end,
            origin_identifier=message.query_origin,
            origin_sent_date=message.origin_sent_date,
            wid=message.wid,
            cid=message.cid,
        )
        sender.parallel_send([query])

    sqs_handle_events(
        events=events,
        message_type=QueryCreateEvent,
        sqs_handler=SQSHandler(queue_url=envars.SQS_CREATE_QUERY),
        function=_sqs_create_query,
    )


def process_sqs_send_query_metadata_listener(events, session: Session) -> None:
    bus = get_global_bus()

    @with_company_session_from_message_reuse_connection(session=session, read_only=False)
    def _sqs_send_query_metadata_listener(
        message: SQSCompanySendMetadata, company_session: Session
    ):
        sender = QuerySenderWS(
            bus=bus,
            company_session=company_session,
            session=session,
        )
        query_repo = QueryRepositorySA(session=company_session)
        creator = QueryCreator(query_repo=query_repo, session=session)
        issued = creator.create(
            company_identifier=message.company_identifier,
            download_type=DownloadType.ISSUED,
            request_type=RequestType.METADATA,
            is_manual=message.manually_triggered,
            wid=message.wid,
            cid=message.cid,
        )
        received = creator.create(
            company_identifier=message.company_identifier,
            download_type=DownloadType.RECEIVED,
            request_type=RequestType.METADATA,
            is_manual=message.manually_triggered,
            wid=message.wid,
            cid=message.cid,
        )
        log(
            Modules.SAT_WS_SYNC_METADATA,
            DEBUG,
            "queries_created",
            {
                "company_identifier": message.company_identifier,
                "issued": issued.identifier,
                "received": received.identifier,
            },
        )

        company = (
            session.query(Company.exceed_metadata_limit)
            .filter(
                Company.identifier == message.company_identifier,
            )
            .one()
        )
        if company.exceed_metadata_limit:
            for query in (issued, received):
                bus.publish(
                    EventType.SAT_SPLIT_NEEDED,
                    QueryNeedToBeSplittedEvent(
                        query=query,
                        company_identifier=query.company_identifier,
                    ),
                )
            return
        sender.parallel_send([issued, received])

    sqs_handle_events(
        events=events,
        message_type=SQSCompanySendMetadata,
        sqs_handler=SQSHandler(queue_url=envars.SQS_SEND_QUERY_METADATA),
        function=_sqs_send_query_metadata_listener,
    )


def process_sqs_verify_query(events, session: Session) -> None:
    """Hydrate verify payloads (``company_identifier``, ``sent_date``, …) then verify against SAT."""
    normalized_events = []
    for event in events:
        body = normalize_verify_query_body(event.body, session)
        normalized_events.append(SimpleNamespace(body=body))
    query_verifier = QueryVerifierWS(
        bus=get_global_bus(),
    )
    queries = []

    def _sqs_verify_query(message: Query):
        queries.append(message)

    sqs_handle_events(
        events=normalized_events,
        message_type=Query,
        sqs_handler=SQSHandler(queue_url=envars.SQS_VERIFY_QUERY),
        function=_sqs_verify_query,
        strict_parse=True,
    )
    query_verifier.parallel_verify(queries)


def process_sqs_download_query(events) -> None:
    package_repo = PackageRepositoryS3(bucket_url=envars.S3_ATTACHMENTS, s3_client=s3_client())
    query_downloader = QueryDownloaderWS(
        bus=get_global_bus(),
        package_repo=package_repo,
    )

    def _sqs_download_query(message: Query):
        query_downloader.download(message)

    sqs_handle_events(
        events=events,
        message_type=Query,
        sqs_handler=SQSHandler(queue_url=envars.SQS_DOWNLOAD_QUERY),
        function=_sqs_download_query,
    )


def process_sqs_process_query_metadata(events, session: Session) -> None:
    package_repo = PackageRepositoryS3(bucket_url=envars.S3_ATTACHMENTS, s3_client=s3_client())
    metadata_repo = MetadataRepositoryZip(
        package_repo=package_repo,
    )

    @with_company_session_from_message_reuse_connection(session=session, read_only=False)
    def _sqs_process_query_metadata(message: Query, company_session: Session):
        query_repo = QueryRepositorySA(session=company_session)
        cfdi_repo = CFDIRepositorySA(session=company_session)
        processor = MetadataProcessor(
            cfdi_repo=cfdi_repo,
            metadata_repo=metadata_repo,
            query_repo=query_repo,
            bus=get_global_bus(),
        )
        rfc = rfc_from_cid(message.company_identifier, session)
        message.company_rfc = rfc
        processor.process(message)

    sqs_handle_events(
        events=events,
        message_type=Query,
        sqs_handler=SQSHandler(queue_url=envars.SQS_PROCESS_PACKAGE_METADATA),
        function=_sqs_process_query_metadata,
        log_event_level=WARNING,
    )


def process_sqs_updater_query(events, session: Session) -> None:
    company_repo = CompanyRepositorySA(session=session)
    bus = get_global_bus()

    @with_company_session_from_message_reuse_connection(session=session, read_only=False)
    def _sqs_updater_query(message: SQSUpdaterQuery, company_session: Session):
        updater = QueryUpdaterWS(
            bus=bus,
            company_session=company_session,
            company_repo=company_repo,
        )
        updater.process_update(message)

    sqs_handle_events(
        events=events,
        message_type=SQSUpdaterQuery,
        sqs_handler=SQSHandler(queue_url=envars.SQS_UPDATER_QUERY),
        function=_sqs_updater_query,
    )


def _get_delay_process_xml(
    rds_verifier,
    query,
    start,
    now,
) -> timedelta:
    if envars.LOCAL_INFRA:
        return timedelta()

    ideal_delay = timedelta()

    elapsed = now - start
    if elapsed > envars.sqs.PROCESS_PACKAGE_XML_WARNING_LAMBDA_TIMEOUT:
        ideal_delay = envars.sqs.MIN_DELAY
        log(
            Modules.PROCESS_XML,
            WARNING,
            "TOO_LONG_PROCESSING_DELAYING",
            {
                "elapsed": elapsed,
                "identifier": query.identifier,
            },
        )
    elif not rds_verifier.is_ok_to_process(max_cpu_utilization=envars.MAX_CPU_UTILIZATION):
        log(
            Modules.PROCESS_XML,
            INFO,
            "RDS_NOT_OK_TO_PROCESS",
            {
                "identifier": query.identifier,
            },
        )
        ideal_delay = timedelta(
            seconds=random.randint(
                int(envars.sqs.PROCESS_PACKAGE_XML_RDS_BUSY_DELAY_MIN.total_seconds()),
                int(envars.sqs.PROCESS_PACKAGE_XML_RDS_BUSY_DELAY_MAX.total_seconds()),
            )
        )
    elif query.execute_at and query.execute_at > now:
        ideal_delay = query.execute_at - now
        log(
            Modules.PROCESS_XML,
            INFO,
            "QUERY_NOT_READY",
            {
                "identifier": query.identifier,
                "execute_at": query.execute_at,
                "ideal_delay": ideal_delay,
            },
        )

    return ideal_delay


def _delay_process_xml_by_execute_at(
    query: Query,
    bus: EventBus,
    ideal_delay: timedelta,
    company_session: Session,
) -> None:
    query.execute_at = datetime.now() + ideal_delay

    if query.state != QueryState.DELAYED:
        state_update_at = utc_now()
        company_session.execute(
            update(SATQueryORM)
            .where(
                SATQueryORM.identifier == query.identifier,
                (SATQueryORM.updated_at < state_update_at) | (SATQueryORM.updated_at.is_(None)),
            )
            .values(
                state=QueryState.DELAYED,
                updated_at=state_update_at,
            )
        )
        query.state = QueryState.DELAYED

    bus.publish(
        EventType.SAT_CFDIS_PROCESS_DELAYED,
        query,
    )


def _process_xml_query(
    query: Query,
    query_repo: QueryRepositorySA,
    processor: XMLProcessor,
) -> None:
    log(
        Modules.PROCESS_XML,
        INFO,
        "XML_QUERY_PROCESSING",
        {"query_identifier": query.identifier},
    )
    query.state = QueryState.PROCESSING
    query_repo.update(query, {"state": query.state})

    processor.process(query)

    query.state = QueryState.PROCESSED
    query_repo.update(query, {"state": query.state})
    log(
        Modules.PROCESS_XML,
        INFO,
        "XML_QUERY_PROCESSED",
        {"query_identifier": query.identifier},
    )


def _sqs_process_query_xml(
    events,
    xml_repo: XMLRepositoryZip,
    session: Session,
    rds_verifier: RDSVerifier,
    bus: EventBus,
):
    start = datetime.now()
    for record in events:
        try:
            query = Query.model_validate_json(record.body)
        except Exception as e:
            log(
                Modules.PROCESS_XML,
                EXCEPTION,
                "XML_QUERY_PARSING_FAILED",
                {"exception": e},
            )
            continue

        delay = _get_delay_process_xml(
            rds_verifier,
            query,
            start,
            datetime.now(),
        )

        with new_company_session_from_company_identifier(
            company_identifier=query.company_identifier,
            session=session,
            read_only=False,
        ) as company_session:
            query_repo = QueryRepositorySA(session=company_session)
            cfdi_repo = CFDIRepositorySA(session=company_session)
            processor = XMLProcessor(
                cfdi_repo=cfdi_repo,
                company_session=company_session,
                xml_repo=xml_repo,
            )

            if delay:
                _delay_process_xml_by_execute_at(query, bus, delay, company_session)
                continue

            rfc = rfc_from_cid(query.company_identifier, session)
            query.company_rfc = rfc
            _process_xml_query(query=query, query_repo=query_repo, processor=processor)


def process_sqs_process_query_xml(events, session: Session) -> None:
    rds_verifier = RDSVerifier(
        statistics_info_time_delta=envars.STATISTICS_INFO_TIME_DELTA,
        statistics_info_period_seconds=envars.STATISTICS_INFO_PERIOD_SECONDS,
        db_cluster_identifier=envars.DB_CLUSTER_IDENTIFIER,
    )
    package_repo = PackageRepositoryS3(bucket_url=envars.S3_ATTACHMENTS, s3_client=s3_client())
    xml_repo = XMLRepositoryZip(
        package_repo=package_repo,
    )

    _sqs_process_query_xml(
        events=events,
        xml_repo=xml_repo,
        rds_verifier=rds_verifier,
        bus=get_global_bus(),
        session=session,
    )


def process_sqs_complete_cfdis(events, session: Session) -> None:
    company_repo = CompanyRepositorySA(session=session)

    @with_company_session_from_message_reuse_connection(session=session, read_only=False)
    def _sqs_complete_cfdis(message: NeedToCompleteCFDIsEvent, company_session: Session):
        query_repo = QueryRepositorySA(session=company_session)

        query_sender = QuerySenderWS(
            bus=get_global_bus(),
            company_session=company_session,
            session=session,
        )
        query_creator = QueryCreator(query_repo=query_repo, session=session)
        completer = QueryCFDISCompleter(
            company_session=company_session,
            query_creator=query_creator,
            query_sender=query_sender,
            company_repo=company_repo,
        )
        completer.complete_cfdis(
            company_identifier=message.company_identifier,
            download_type=DownloadType(message.download_type),
            is_manual=message.is_manual,
            start=message.start,
            end=message.end,
        )

    sqs_handle_events(
        events=events,
        message_type=NeedToCompleteCFDIsEvent,
        sqs_handler=SQSHandler(queue_url=envars.SQS_COMPLETE_CFDIS),
        function=_sqs_complete_cfdis,
    )


def process_sqs_to_export(events, session: Session) -> None:
    @with_company_session_from_message_reuse_connection(session=session, read_only=False)
    def _sqs_to_export(message: SQSMessagePayload, company_session: Session):
        exporter = CFDIExporter(
            company_session=company_session,
            cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
            bus=get_global_bus(),
        )

        json_body = message.json_body
        body = _export_search_attrs_from_json(json_body)
        resume_type = ResumeType[json_body.get("TipoDeComprobante", ResumeType.BASIC)]
        export_data = json_body.get("export_data")

        cfdi_export_identifier = json_body["cfdi_export_identifier"]
        body["limit"] = None
        body["offset"] = None
        fields = json_body.get("fields", [])
        if not fields:
            fields = list({"UUID", "xml_content"})
        body["fields"] = fields

        exporter.export(cfdi_export_identifier, body, fields, None, resume_type, export_data)

    sqs_handle_events(
        events=events,
        message_type=SQSMessagePayload,
        sqs_handler=SQSHandler(queue_url=envars.SQS_EXPORT),
        function=_sqs_to_export,
    )


def process_sqs_massive_export(events, session: Session) -> None:
    @with_company_session_from_message_reuse_connection(session=session, read_only=False)
    def _sqs_massive_export(message: SQSMessagePayload, company_session: Session):
        exporter = CFDIExporter(
            company_session=company_session,
            cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
            export_repo_s3=ExportRepositoryS3(),
            bus=get_global_bus(),
        )

        json_body = message.json_body
        identifier = json_body["identifier"]
        export_data = json_body["export_data"]
        nested_json_body = json_body["json_body"]

        exporter.handle_export_type(
            cfdi_export_identifier=identifier, export_data=export_data, body=nested_json_body
        )

    sqs_handle_events(
        events=events,
        message_type=SQSMessagePayload,
        sqs_handler=SQSHandler(queue_url=envars.SQS_MASSIVE_EXPORT),
        function=_sqs_massive_export,
    )


def get_sat_local_poll_dispatchers() -> dict[str, tuple]:
    """Queue URL → (callable(event_dict, context), display label) for local SQS pollers."""
    from chalicelib.new.utils.session import new_session

    def _wrap_with_session(comment: str, read_only: bool, fn):
        def _run(event_dict, context):
            with new_session(comment=comment, read_only=read_only) as session:
                fn(event_dict, session)

        return _run

    def _create(event_dict, session):
        process_sqs_create_query(dict_to_sqs_event_records(event_dict), session)

    def _send_meta(event_dict, session):
        process_sqs_send_query_metadata_listener(dict_to_sqs_event_records(event_dict), session)

    def _verify(event_dict, session):
        process_sqs_verify_query(dict_to_sqs_event_records(event_dict), session)

    def _download(event_dict, _context):
        process_sqs_download_query(dict_to_sqs_event_records(event_dict))

    def _updater(event_dict, session):
        process_sqs_updater_query(dict_to_sqs_event_records(event_dict), session)

    def _metadata(event_dict, session):
        process_sqs_process_query_metadata(dict_to_sqs_event_records(event_dict), session)

    def _complete(event_dict, session):
        process_sqs_complete_cfdis(dict_to_sqs_event_records(event_dict), session)

    def _xml(event_dict, session):
        process_sqs_process_query_xml(dict_to_sqs_event_records(event_dict), session)

    def _export(event_dict, session):
        process_sqs_to_export(dict_to_sqs_event_records(event_dict), session)

    def _massive(event_dict, session):
        process_sqs_massive_export(dict_to_sqs_event_records(event_dict), session)

    return {
        envars.SQS_CREATE_QUERY: (
            _wrap_with_session("sqs_create_query", False, _create),
            "Create Query",
        ),
        envars.SQS_SEND_QUERY_METADATA: (
            _wrap_with_session("sqs_send_query_metadata_listener", False, _send_meta),
            "Send Metadata",
        ),
        envars.SQS_VERIFY_QUERY: (
            _wrap_with_session("sqs_verify_query", False, _verify),
            "Verify",
        ),
        envars.SQS_DOWNLOAD_QUERY: (_download, "Download"),
        envars.SQS_UPDATER_QUERY: (
            _wrap_with_session("sqs_updater_query", False, _updater),
            "Update Query",
        ),
        envars.SQS_PROCESS_PACKAGE_METADATA: (
            _wrap_with_session("sqs_process_query_metadata", False, _metadata),
            "Process Metadata",
        ),
        envars.SQS_COMPLETE_CFDIS: (
            _wrap_with_session("sqs_complete_cfdis", False, _complete),
            "Complete CFDIs",
        ),
        envars.SQS_PROCESS_PACKAGE_XML: (
            _wrap_with_session("sqs_process_query_xml", False, _xml),
            "Process XML",
        ),
        envars.SQS_EXPORT: (
            _wrap_with_session("sqs_to_export", False, _export),
            "Export",
        ),
        envars.SQS_MASSIVE_EXPORT: (
            _wrap_with_session("sqs_massive_export", False, _massive),
            "Massive Export",
        ),
    }
