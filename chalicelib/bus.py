import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

import stripe

from chalicelib.boto3_clients import s3_client
from chalicelib.logger import DEBUG, ERROR, log
from chalicelib.modules import Modules
from chalicelib.new.cfdi_processor.infra.messages.need_to_complete_cfdis import (
    NeedToCompleteCFDIsEvent,
)
from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.chunk import Chunk
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.query.domain.query import Query
from chalicelib.new.query.domain.query_creator import last_X_fiscal_years
from chalicelib.new.query.infra.query_need_split_handler import QueryNeedSplitHandler
from chalicelib.new.scraper.domain.events.sqs_request_new_scrap import (
    CompanyScrapEvent,
    ScrapRequest,
)
from chalicelib.new.scraper.utils import chunks_to_subchunks, prepare_to_scrap
from chalicelib.new.shared.domain.event.event import CompanyWithSession
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.event.event_handler import EventHandler
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.infra.message.sqs_company import SQSCompanySendMetadata
from chalicelib.new.shared.infra.sqs_handler import SQSHandler
from chalicelib.new.sqs_local import SQSClientLocal
from chalicelib.new.stripe.infra import StripeConfig
from chalicelib.new.stripe.infra.stripe_subscription_creator import (
    StripeSubscriptionCreator,
)
from chalicelib.new.utils.datetime import mx_now
from chalicelib.new.ws_sat.infra.sqs_process_query import (
    set_execute_at,
)
from chalicelib.schema.models.user import User

_bus: EventBus = None


def random_execute_at_from_now(max_delay: timedelta) -> datetime:
    random_delay = timedelta(seconds=random.randint(0, int(max_delay.total_seconds())))
    return datetime.now() + random_delay


def get_global_bus():
    global _bus  # pylint: disable=global-statement
    if _bus is None:
        _bus = EventBus()
    return _bus


@dataclass
class OnCompanyCreateAutoSync(EventHandler):
    bus: EventBus

    def handle(self, company_with_session: CompanyWithSession):
        company = company_with_session.company
        start = last_X_fiscal_years(years=5)

        # Always run webservice (SAT metadata download)
        self.bus.publish(
            EventType.SAT_METADATA_REQUESTED,  # WebService
            SQSCompanySendMetadata(
                company_identifier=company.identifier,
                manually_triggered=True,
                wid=company.workspace_id,
                cid=company.id,
            ),
        )

        event_issued = NeedToCompleteCFDIsEvent(
            company_identifier=company.identifier,
            download_type=DownloadType.ISSUED,
            is_manual=False,
            start=start,
        )
        self.bus.publish(EventType.SAT_COMPLETE_CFDIS_NEEDED, event_issued)
        event_received = NeedToCompleteCFDIsEvent(
            company_identifier=company.identifier,
            download_type=DownloadType.RECEIVED,
            is_manual=False,
            start=start,
        )
        self.bus.publish(EventType.SAT_COMPLETE_CFDIS_NEEDED, event_received)

        # Skip scraper in local development (only webservice runs)
        if envars.LOCAL_INFRA:
            return

        fecha_inicio = envars.SCRAP_MANUAL_START_DATE

        chunks = [
            (
                Chunk(
                    start=fecha_inicio,
                    end=mx_now(),
                ),
                DownloadType.RECEIVED,
            ),
            (
                Chunk(
                    start=fecha_inicio,
                    end=mx_now(),
                ),
                DownloadType.ISSUED,
            ),
        ]
        subchunks = chunks_to_subchunks(chunks)
        self.bus.publish(
            EventType.REQUEST_SCRAP,
            ScrapRequest(
                company=company,
                company_session=company_with_session.company_session,
                chunks=subchunks,
            ),
        )


@dataclass
class RequestScrap(EventHandler):
    bus: EventBus

    def handle(self, scrap_request: ScrapRequest):
        company = scrap_request.company
        company_session = scrap_request.company_session
        request = CompanyScrapEvent(
            company_identifier=company.identifier,
            wid=company.workspace_id,
            cid=company.id,
            start_metadata_cancel=scrap_request.start_metadata_cancel,
            end_metadata_cancel=scrap_request.end_metadata_cancel,
            chunks=scrap_request.chunks,
        )
        prepare_to_scrap(
            company_session=company_session,
            s3_client=s3_client(),
            requests=[request],
            is_manual=True,
        )
        self.bus.publish(EventType.SQS_SCRAP_ORCHESTRATOR, request)


@dataclass
class OnFirstCompanyCreatedRestoreTrial(EventHandler, StripeConfig):
    def handle(self, user: User):
        if not user.stripe_subscription_identifier:
            return

        current_subscription = stripe.Subscription.retrieve(user.stripe_subscription_identifier)

        if current_subscription.plan.product != envars.VITE_REACT_APP_PRODUCT_TRIAL:
            return

        subcription_cancel_at = current_subscription.cancel_at
        today = datetime.now().date()
        today_timestamp = int(time.mktime(today.timetuple()))
        if (
            current_subscription
            and subcription_cancel_at
            and subcription_cancel_at > today_timestamp
        ):
            stripe.Subscription.delete(user.stripe_subscription_identifier)
        stripe_account_creator = StripeSubscriptionCreator()
        stripe_account_creator.new_subscription(
            user, cancel_delta=envars.STRIPE_DEFAULT_CANCEL_AT_DELTA
        )
        if user.source_name:
            stripe.Customer.modify(user.stripe_identifier, coupon=envars.STRIPE_COUPON)


@dataclass
class OnQueryReadyToDownloadProcessQuery(EventHandler):
    bus: EventBus

    def handle(self, event: Query):
        log(
            Modules.SAT_WS_DOWNLOAD,
            DEBUG,
            "DOWNLOADED",
            {
                "identifier": event.identifier,
            },
        )
        if event.request_type == RequestType.METADATA:
            log(
                Modules.SAT_WS_DOWNLOAD,
                DEBUG,
                "SENDING_TO_PROCESS_METADATA",
                {
                    "identifier": event.identifier,
                },
            )
            event.execute_at = random_execute_at_from_now(envars.sqs.PROCESS_METADATA_MAX_DELAY)
            self.bus.publish(
                EventType.SAT_METADATA_DOWNLOADED,
                event,
            )
        elif event.request_type == RequestType.CFDI:
            set_execute_at(event)
            log(
                Modules.SAT_WS_DOWNLOAD,
                DEBUG,
                "SENDING_TO_PROCESS_XML",
                {
                    "identifier": event.identifier,
                },
            )
            self.bus.publish(
                EventType.SAT_CFDIS_DOWNLOADED,
                event,
            )
        else:
            log(
                Modules.SAT_WS_DOWNLOAD,
                ERROR,
                "INVALID_REQUEST_TYPE",
                {
                    "identifier": event.identifier,
                },
            )


_local_infra_functions: dict[str, Callable]


def local_infra_functions(relations: dict[str, Callable]):
    global _local_infra_functions  # pylint: disable=global-statement
    _local_infra_functions = relations


def replace_infra_with_local():
    """
    Replace SQS handlers with local in-memory clients for LOCAL_INFRA mode.
    
    IMPORTANT: Processing queues (PROCESS_PACKAGE_METADATA, PROCESS_PACKAGE_XML)
    are excluded so they send real messages to LocalStack SQS for the worker to process.
    """
    bus = get_global_bus()
    all_handlers = []
    for handlers in bus.handlers.values():
        all_handlers.extend(iter(handlers))
    
    # Queues that should send to LocalStack SQS (not in-memory)
    queues_for_localstack = {
        envars.SQS_PROCESS_PACKAGE_METADATA,
        envars.SQS_PROCESS_PACKAGE_XML,
    }
    
    for handler in all_handlers:
        if isinstance(handler, SQSHandler) and handler.queue_url in _local_infra_functions:
            # Skip processing queues - they should use real LocalStack SQS
            if handler.queue_url in queues_for_localstack:
                continue
            handler.sqs_client = SQSClientLocal(_local_infra_functions[handler.queue_url])


sqs_handlers: tuple[tuple[EventType, str], ...] = (
    # Company
    # SAT
    (EventType.SAT_METADATA_REQUESTED, envars.SQS_SEND_QUERY_METADATA),
    (EventType.SAT_METADATA_DOWNLOADED, envars.SQS_PROCESS_PACKAGE_METADATA),
    (EventType.SAT_WS_QUERY_SENT, envars.SQS_VERIFY_QUERY),
    (EventType.SAT_WS_QUERY_VERIFY_NEEDED, envars.SQS_VERIFY_QUERY),
    (EventType.SAT_WS_QUERY_DOWNLOAD_READY, envars.SQS_DOWNLOAD_QUERY),
    (EventType.WS_UPDATER, envars.SQS_UPDATER_QUERY),
    (EventType.SAT_WS_REQUEST_CREATE_QUERY, envars.SQS_CREATE_QUERY),
    (EventType.SAT_CFDIS_DOWNLOADED, envars.SQS_PROCESS_PACKAGE_XML),  # TODO unify),
    (
        EventType.SAT_CFDIS_PROCESS_DELAYED,
        envars.SQS_PROCESS_PACKAGE_XML,
    ),  # TODO unify),
    (EventType.SAT_COMPLETE_CFDIS_NEEDED, envars.SQS_COMPLETE_CFDIS),
    (EventType.SQS_SCRAP_ORCHESTRATOR, envars.SQS_SCRAP_ORCHESTRATOR),
    (EventType.SQS_SCRAP_DELAY, envars.SQS_SCRAP_DELAYER),
    # ADD
    (EventType.ADD_METADATA_REQUESTED, envars.SQS_ADD_METADATA_REQUEST),
    (EventType.ADD_METADATA_DOWNLOADED, envars.SQS_ADD_PROCESS_METADATA),
    (EventType.ADD_SYNC_REQUEST_CREATED, envars.SQS_ADD_DATA_SYNC),
    # PASTO
    (EventType.PASTO_WORKER_CREATED, envars.SQS_PASTO_CONFIG_WORKER),
    (EventType.PASTO_WORKER_CREDENTIALS_SET, envars.SQS_PASTO_GET_COMPANIES),
    (EventType.PASTO_RESET_LICENSE_KEY_REQUESTED, envars.SQS_RESET_ADD_LICENSE_KEY),
    # EXPORT
    (EventType.USER_EXPORT_CREATED, envars.SQS_EXPORT),
    (EventType.MASSIVE_EXPORT_CREATED, envars.SQS_MASSIVE_EXPORT),
    # SCRAPER
    (EventType.SAT_SCRAP_PDF, envars.SQS_SAT_SCRAP_PDF),
    # NOTIFICATIONS
    (EventType.NOTIFICATIONS, envars.SQS_NOTIFICATIONS),
    # COI
    (EventType.COI_METADATA_UPLOADED, envars.SQS_COI_METADATA_UPLOADED),
    (EventType.COI_SYNC_DATA, envars.SQS_COI_DATA_SYNC),
)


def _subscribe_sqs(bus, sqs_handlers):
    for event_type, url in sqs_handlers:
        bus.subscribe(
            event_type=event_type,
            handler=SQSHandler(queue_url=url),
        )


def suscribe_all_handlers():
    bus = get_global_bus()
    _subscribe_sqs(bus, sqs_handlers)
    bus.subscribe(
        event_type=EventType.COMPANY_CREATED,
        handler=OnCompanyCreateAutoSync(bus=bus),
    )
    bus.subscribe(
        event_type=EventType.REQUEST_SCRAP,
        handler=RequestScrap(bus=bus),
    )
    bus.subscribe(
        event_type=EventType.REQUEST_RESTORE_TRIAL,
        handler=OnFirstCompanyCreatedRestoreTrial(),
    )
    bus.subscribe(
        event_type=EventType.SAT_WS_QUERY_DOWNLOADED,
        handler=OnQueryReadyToDownloadProcessQuery(bus=bus),
    )
    bus.subscribe(
        event_type=EventType.SAT_SPLIT_NEEDED,
        handler=QueryNeedSplitHandler(bus=bus),
    )
    # LOCAL_INFRA: All messages route through real LocalStack SQS queues.
    # replace_infra_with_local() (in-memory direct calls) is intentionally skipped
    # because LocalStack is running and the SQS worker polls all queues.
    # Enabling it caused recursive calls when SAT re-enqueued pending verifications.
