import enum

from chalicelib.modules import NameEnum


class RequestType(NameEnum):
    CFDI = enum.auto()
    METADATA = enum.auto()
    BOTH = enum.auto()
    CANCELLATION = enum.auto()
