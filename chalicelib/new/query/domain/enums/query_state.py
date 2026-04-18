import enum


class QueryState(enum.Enum):
    # Temporary
    DRAFT = "DRAFT"
    SENT = "SENT"
    TO_DOWNLOAD = "TO_DOWNLOAD"
    DOWNLOADED = "DOWNLOADED"
    TO_SCRAP = "TO_SCRAP"
    DELAYED = "DELAYED"
    PROCESSING = "PROCESSING"

    # Final Problematic
    ## Sent
    ERROR_IN_CERTS = "ERROR_IN_CERTS"
    ERROR_SAT_WS_UNKNOWN = "ERROR_SAT_WS_UNKNOWN"
    ERROR_SAT_WS_INTERNAL = "ERROR_SAT_WS_INTERNAL"
    ## Verifying
    ERROR_TOO_BIG = "ERROR_TOO_BIG"
    TIME_LIMIT_REACHED = "TIME_LIMIT_REACHED"
    ## Processing
    ## Other
    ERROR = "ERROR"
    SCRAP_FAILED = "SCRAP_FAILED"
    CANT_SCRAP = "CANT_SCRAP"
    MANUALLY_CANCELLED = "MANUALLY_CANCELLED"
    # Final Not Success
    SPLITTED = "SPLITTED"
    INFORMATION_NOT_FOUND = "INFORMATION_NOT_FOUND"
    SUBSTITUTED = "SUBSTITUTED"
    SUBSTITUTED_TO_SCRAP = "SUBSTITUTED_TO_SCRAP"

    # Final Success
    PROCESSED = "PROCESSED"
    SCRAPPED = "SCRAPPED"


FinalOkStates = {QueryState.PROCESSED}

InProgressStates = {
    QueryState.TO_DOWNLOAD,
    QueryState.DOWNLOADED,
    QueryState.PROCESSING,
    QueryState.DELAYED,
}

FinalStates = {
    *FinalOkStates,
    QueryState.ERROR,
    QueryState.CANT_SCRAP,
    QueryState.SPLITTED,
    QueryState.INFORMATION_NOT_FOUND,
    QueryState.MANUALLY_CANCELLED,
    QueryState.SCRAP_FAILED,
    QueryState.TIME_LIMIT_REACHED,
    QueryState.SUBSTITUTED,
    QueryState.SUBSTITUTED_TO_SCRAP,
}
