from enum import StrEnum, auto


class NameEnum(StrEnum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        return name


class Modules(NameEnum):
    ATTACHMENT = auto()
    ACCOUNT = auto()
    ADD = auto()
    ADD_CANCEL = auto()
    ADD_FULL = auto()
    ADD_METADATA = auto()
    ADD_METADATA_WEBHOOK = auto()
    ADD_WEBHOOK = auto()
    ADD_XML = auto()
    BUS = auto()
    CFDI_CONTROLLER = auto()
    DB = auto()
    EFOS = auto()
    EXPORT = auto()
    EXPORT_MASSIVE = auto()
    FIEL = auto()
    GENERATE_XML_REQUESTS = auto()
    LICENSE = auto()
    NOTIFICATION = auto()
    ODOO = auto()
    PERMISSION = auto()
    PROCESS_METADATA = auto()
    PROCESS_PAYMENTS = auto()
    PROCESS_XML = auto()
    REVERIFY = auto()
    ROOT = auto()
    SAT_WS_SPLIT = auto()
    SAT_WS_COMPLETE_CFDI = auto()
    SAT_WS_SYNC_METADATA = auto()
    SAT_WS_REVERIFY_OLD = auto()
    SAT_WS_CREATE_QUERY = auto()
    SAT_WS_DOWNLOAD = auto()
    SAT_WS_SEND = auto()
    SAT_WS_VERIFY = auto()
    SAT_QUERY_CLEAN = auto()
    SCRAPER = auto()
    SCRAPER_PDF = auto()
    SCRAPER_PROCESS = auto()
    SCRAPER_PROCESS_2 = auto()
    SCRAPER_PROCESS_XML = auto()
    SEARCH = auto()
    SQS_HANDLER = auto()
    STRIPE = auto()
    USER_API = auto()
    IVA = auto()
    RESUME = auto()
    IN_OPERATOR = auto()
    CRON = auto()
    COI = auto()
    MARKETING_EMAIL = auto()

    @classmethod
    def endpoint(cls, endpoint: str) -> str:
        return f"end_{endpoint}"
