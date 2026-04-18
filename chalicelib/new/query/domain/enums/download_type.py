from chalicelib.modules import NameEnum


class DownloadType(NameEnum):
    ISSUED = "ISSUED"
    RECEIVED = "RECEIVED"
    BOTH = "BOTH"

    def to_bool(self) -> bool:
        if self == DownloadType.ISSUED:
            return True
        if self == DownloadType.RECEIVED:
            return False
        return None

    @classmethod
    def from_bool(cls, value: bool) -> "DownloadType":
        if value is None:
            return DownloadType.BOTH
        return DownloadType.ISSUED if value else DownloadType.RECEIVED

    @property
    def postfix(self) -> str:
        if self == DownloadType.ISSUED:
            return "issued"
        if self == DownloadType.RECEIVED:
            return "not_issued"
        raise ValueError(f"Invalid value: {self}")
