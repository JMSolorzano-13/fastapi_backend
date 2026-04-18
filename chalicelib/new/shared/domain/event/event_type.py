import enum


class EventType(enum.Enum):  # TODO assume from event class
    """Event types

    Glossary:
        - REQUESTED: Request to our self service to do something, generally via SQS
    """

    # Company
    COMPANY_CREATED = enum.auto()
    REQUEST_RESTORE_TRIAL = enum.auto()
    # SAT
    SAT_METADATA_REQUESTED = enum.auto()
    SAT_METADATA_DOWNLOADED = enum.auto()
    WS_UPDATER = enum.auto()
    SAT_METADATA_PROCESSED = enum.auto()
    SAT_CFDIS_DOWNLOADED = enum.auto()
    SAT_CFDIS_PROCESS_DELAYED = enum.auto()
    SAT_WS_QUERY_DOWNLOADED = enum.auto()
    SAT_SCRAP_NEEDED = enum.auto()
    SAT_SPLIT_NEEDED = enum.auto()
    SAT_WS_QUERY_DOWNLOAD_READY = enum.auto()
    SAT_WS_QUERY_VERIFY_NEEDED = enum.auto()
    SAT_WS_QUERY_SENT = enum.auto()
    SAT_WS_REQUEST_CREATE_QUERY = enum.auto()
    SAT_COMPLETE_CFDIS_NEEDED = enum.auto()
    SAT_COMPLETE_CFDIS_SCRAP_NEEDED = enum.auto()
    SQS_SCRAP_ORCHESTRATOR = enum.auto()
    # ADD
    ADD_METADATA_REQUESTED = enum.auto()
    ADD_METADATA_DOWNLOADED = enum.auto()
    ADD_SYNC_REQUEST_CREATED = enum.auto()
    # PASTO
    PASTO_RESET_LICENSE_KEY_REQUESTED = enum.auto()
    PASTO_WORKER_CREATED = enum.auto()
    PASTO_WORKER_CREDENTIALS_SET = enum.auto()
    # EXPORT
    USER_EXPORT_CREATED = enum.auto()
    MASSIVE_EXPORT_CREATED = enum.auto()
    # SCRAPER
    SAT_SCRAP_PDF = enum.auto()
    SQS_SCRAP_DELAY = enum.auto()
    REQUEST_SCRAP = enum.auto()
    # NOTIFICATIONS
    NOTIFICATIONS = enum.auto()
    # COI
    COI_METADATA_UPLOADED = enum.auto()
    COI_SYNC_DATA = enum.auto()
