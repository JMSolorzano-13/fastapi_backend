import enum


class CfdiExportState(enum.Enum):
    SENT = "SENT"
    TO_DOWNLOAD = "TO_DOWNLOAD"
    ERROR = "ERROR"
