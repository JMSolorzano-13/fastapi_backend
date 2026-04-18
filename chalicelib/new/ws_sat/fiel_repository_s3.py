import enum
import io
from typing import Any

from botocore.exceptions import ClientError
from pycfdi_credentials import Certificate, PrivateKey


# Enum igual que antes
class FileType(enum.Enum):
    CERTIFICATE = "certificate.cer"
    PRIVATE_KEY = "private.key"
    PASSPHRASE = "passphrase.txt"


# Excepción personalizada
class CertsNotFound(Exception):
    pass


def get_route(wid: int, cid: int, file_type: FileType) -> str:
    ext = file_type.value.split(".")[-1]
    return f"ws_{wid}/c_{cid}.{ext}"


def get_files(s3_client: Any, bucket_url: str, wid: int, cid: int) -> tuple[bytes, bytes, bytes]:
    cer_file = io.BytesIO()
    key_file = io.BytesIO()
    txt_file = io.BytesIO()
    routes = {
        FileType.CERTIFICATE: get_route(wid, cid, FileType.CERTIFICATE),
        FileType.PRIVATE_KEY: get_route(wid, cid, FileType.PRIVATE_KEY),
        FileType.PASSPHRASE: get_route(wid, cid, FileType.PASSPHRASE),
    }
    try:
        s3_client.download_fileobj(
            bucket_url,
            routes[FileType.CERTIFICATE],
            cer_file,
        )
        s3_client.download_fileobj(
            bucket_url,
            routes[FileType.PRIVATE_KEY],
            key_file,
        )
        s3_client.download_fileobj(
            bucket_url,
            routes[FileType.PASSPHRASE],
            txt_file,
        )
    except ClientError as e:
        raise CertsNotFound from e
    cer_file.seek(0)
    key_file.seek(0)
    txt_file.seek(0)
    return (
        cer_file.read(),
        key_file.read(),
        txt_file.read(),
    )


def _check_certs_exist(s3_client: Any, bucket_url: str, wid: int, cid: int) -> bool:
    """Check if certificates exist without raising exceptions"""
    return certs_exist(s3_client=s3_client, bucket_url=bucket_url, wid=wid, cid=cid)


def certs_exist(s3_client: Any, bucket_url: str, wid: int, cid: int) -> bool:
    routes = {
        FileType.CERTIFICATE: get_route(wid, cid, FileType.CERTIFICATE),
        FileType.PRIVATE_KEY: get_route(wid, cid, FileType.PRIVATE_KEY),
        FileType.PASSPHRASE: get_route(wid, cid, FileType.PASSPHRASE),
    }

    try:
        for route in routes.values():
            s3_client.head_object(Bucket=bucket_url, Key=route)
        return True
    except ClientError:
        return False


def get_fiel_from_wid_cid(s3_client: Any, bucket_url: str, wid: int, cid: int):
    try:
        cer, key, txt = get_files(s3_client, bucket_url, wid, cid)
    except CertsNotFound:
        raise
    certificate = Certificate(
        certificate=cer,
    )
    private_key = PrivateKey(
        content=key,
        passphrase=txt,
    )
    return {
        "certificate": certificate,
        "private_key": private_key,
    }
