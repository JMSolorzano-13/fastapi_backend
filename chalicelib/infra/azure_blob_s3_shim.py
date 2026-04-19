"""Minimal S3-shaped facade over Azure Blob Storage (container = bucket, blob name = key).

Used when ``LOCAL_INFRA`` is off and ``AZURE_STORAGE_CONNECTION_STRING`` is set.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, BinaryIO
from urllib.parse import quote

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas
from botocore.exceptions import ClientError


def _parse_account_credentials(connection_string: str) -> tuple[str, str]:
    parts: dict[str, str] = {}
    for segment in connection_string.split(";"):
        segment = segment.strip()
        if not segment or "=" not in segment:
            continue
        k, v = segment.split("=", 1)
        parts[k.strip()] = v.strip()
    try:
        return parts["AccountName"], parts["AccountKey"]
    except KeyError as e:
        raise ValueError(
            "AZURE_STORAGE_CONNECTION_STRING must include AccountName and AccountKey"
        ) from e


def _encode_blob_path(key: str) -> str:
    return "/".join(quote(part, safe="") for part in key.split("/"))


class AzureBlobS3Shim:
    """Subset of boto3 S3 client methods used by the FastAPI backend."""

    def __init__(self, connection_string: str) -> None:
        self._conn = connection_string
        self._account_name, self._account_key = _parse_account_credentials(connection_string)
        self._bsc = BlobServiceClient.from_connection_string(connection_string)

    def upload_fileobj(
        self,
        Fileobj: BinaryIO,
        Bucket: str,
        Key: str,
        **kwargs: Any,
    ) -> None:
        data = Fileobj.read()
        self._bsc.get_blob_client(container=Bucket, blob=Key).upload_blob(data, overwrite=True)

    def put_object(self, Bucket: str, Key: str, Body: bytes, **kwargs: Any) -> None:
        self._bsc.get_blob_client(container=Bucket, blob=Key).upload_blob(Body, overwrite=True)

    def download_fileobj(self, Bucket: str, Key: str, Fileobj: BinaryIO, **kwargs: Any) -> None:
        try:
            data = self._bsc.get_blob_client(container=Bucket, blob=Key).download_blob().readall()
        except ResourceNotFoundError as e:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "GetObject",
            ) from e
        Fileobj.write(data)

    def delete_object(self, Bucket: str, Key: str, **kwargs: Any) -> None:
        self._bsc.get_blob_client(container=Bucket, blob=Key).delete_blob()

    def head_object(self, Bucket: str, Key: str, **kwargs: Any) -> dict[str, Any]:
        try:
            props = self._bsc.get_blob_client(container=Bucket, blob=Key).get_blob_properties()
        except ResourceNotFoundError as e:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            ) from e
        return {
            "ContentLength": props.size,
            "LastModified": props.last_modified,
        }

    def copy(self, CopySource: dict[str, str], Bucket: str, Key: str, **kwargs: Any) -> None:
        src_bucket = CopySource["Bucket"]
        src_key = CopySource["Key"]
        source = self._bsc.get_blob_client(container=src_bucket, blob=src_key)
        dest = self._bsc.get_blob_client(container=Bucket, blob=Key)
        try:
            data = source.download_blob().readall()
        except ResourceNotFoundError as e:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "CopyObject",
            ) from e
        dest.upload_blob(data, overwrite=True)

    def upload_file(self, Filename: str, Bucket: str, Key: str, **kwargs: Any) -> None:
        with open(Filename, "rb") as f:
            self.upload_fileobj(f, Bucket, Key)

    def download_file(self, Bucket: str, Key: str, Filename: str, **kwargs: Any) -> None:
        with open(Filename, "wb") as f:
            self.download_fileobj(Bucket, Key, f)

    def generate_presigned_url(
        self,
        ClientMethod: str,
        Params: dict[str, Any] | None = None,
        ExpiresIn: int = 3600,
        HttpMethod: str | None = None,
        **kwargs: Any,
    ) -> str:
        Params = Params or {}
        bucket = Params["Bucket"]
        key = Params["Key"]
        if ClientMethod == "put_object":
            permission = BlobSasPermissions(
                read=True,
                write=True,
                create=True,
                add=True,
            )
        else:
            permission = BlobSasPermissions(read=True)

        expiry = datetime.now(UTC) + timedelta(seconds=int(ExpiresIn))
        sas = generate_blob_sas(
            account_name=self._account_name,
            container_name=bucket,
            blob_name=key,
            account_key=self._account_key,
            permission=permission,
            expiry=expiry,
        )
        encoded = _encode_blob_path(key)
        base = f"https://{self._account_name}.blob.core.windows.net/{quote(bucket)}/{encoded}"
        return f"{base}?{sas}"
