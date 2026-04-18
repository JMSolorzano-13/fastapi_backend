from io import BytesIO
from zipfile import ZipFile

from ....core import CFDI


class MissingData(ValueError):
    pass


class CFDIParser:
    @classmethod
    def _get_files(cls, zipfile: ZipFile) -> list[str]:
        return [zipfile.read(name).decode() for name in zipfile.namelist()]

    @classmethod
    def from_binary(cls, binary: bytes) -> list[CFDI]:
        zipfile = ZipFile(BytesIO(binary))
        return cls.parse_zip(zipfile)

    @classmethod
    def parse_zip(cls, zipfile: ZipFile) -> list[CFDI]:
        raise NotImplementedError
