from enum import StrEnum, auto

SPLIT_SIZE = 10_000
RE_VERIFY_OLD_SPLIT_SIZE = 100


class func_name(StrEnum):
    LM_RE_VERIFY_OLD = auto()
    LM_SEND_NOTIFICATION_EMAILS = auto()
    LM_SYNC_METADATA = auto()
    LM_COMPLETE_CFDIS = auto()
    LM_CLEAN_SAT_QUERY = auto()
    LM_ADD_SYNC_METADATA = auto()
    LM_SCRAP_CRON = auto()
    LM_MARKETING_EMAIL = auto()
