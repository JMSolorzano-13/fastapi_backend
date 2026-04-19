"""Object storage and legacy AWS-shaped clients.

- **Cloud** (``LOCAL_INFRA=0``): ``s3_client()`` is ``AzureBlobS3Shim``
  (``chalicelib.infra.azure_blob_s3_shim``; needs ``AZURE_STORAGE_CONNECTION_STRING``).
  Other AWS-shaped helpers are local-only; use Azure in cloud.

- **Local** (``LOCAL_INFRA=1``): :mod:`chalicelib.infra.localstack_boto_clients` (boto3).
"""

from __future__ import annotations

import io
import json
import os
from functools import lru_cache
from typing import Any, BinaryIO

from chalicelib.new.config.infra import envars


def _is_local() -> bool:
    return bool(envars.LOCAL_INFRA)


_azure_s3: Any | None = None


def s3_client() -> Any:
    """S3-compatible client: LocalStack (local) or Azure Blob (cloud)."""
    global _azure_s3
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import s3_client as _s3

        return _s3()
    if _azure_s3 is None:
        from chalicelib.infra.azure_blob_s3_shim import AzureBlobS3Shim

        conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "").strip()
        if not conn:
            raise RuntimeError(
                "Azure object storage: set AZURE_STORAGE_CONNECTION_STRING "
                "(or LOCAL_INFRA=1 for LocalStack S3)."
            )
        _azure_s3 = AzureBlobS3Shim(conn)
    return _azure_s3


def upload_fileobj_to_object_storage(bucket: str, key: str, fileobj: BinaryIO) -> None:
    data = fileobj.read()
    s3_client().upload_fileobj(io.BytesIO(data), bucket, key)


def sqs_client() -> Any:
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import sqs_client as _sqs

        return _sqs()
    raise RuntimeError(
        "sqs_client() is LOCAL_INFRA-only; use Azure Service Bus via queue_transport in cloud."
    )


def ses_client() -> Any:
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import ses_client as _ses

        return _ses()
    raise RuntimeError(
        "ses_client() is LOCAL_INFRA-only; configure mail on Azure outside boto3_clients."
    )


def secretsmanager_client() -> Any:
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import secretsmanager_client as _sm

        return _sm()
    raise RuntimeError("secretsmanager_client() is LOCAL_INFRA-only.")


def _secret_override_env_key(secret_id: str) -> str:
    normalized = "".join(c if c.isalnum() else "_" for c in secret_id).upper().strip("_")
    return f"AWSSM_JSON_{normalized}"


@lru_cache
def secretsmanager_get(secret_id: str) -> dict[str, Any]:
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import secretsmanager_get as _get

        return _get(secret_id)
    env_key = _secret_override_env_key(secret_id)
    raw = os.environ.get(env_key)
    if raw:
        return json.loads(raw)
    raise RuntimeError(
        f"secretsmanager_get({secret_id!r}): set env {env_key} to the JSON secret string "
        "(ACA / Key Vault), or use LOCAL_INFRA=1 with LocalStack."
    )


def lambda_client() -> Any:
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import lambda_client as _l

        return _l()
    raise RuntimeError("lambda_client() is LOCAL_INFRA-only.")


def lambda_client_pdf() -> Any:
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import lambda_client_pdf as _lp

        return _lp()
    raise RuntimeError("lambda_client_pdf() is LOCAL_INFRA-only.")


def cognito_client() -> Any:
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import cognito_client as _c

        return _c()
    raise RuntimeError("cognito_client() is LOCAL_INFRA-only; cloud auth uses selfauth / JWT.")


def cloudwatch_client() -> Any:
    if _is_local():
        from chalicelib.infra.localstack_boto_clients import cloudwatch_client as _cw

        return _cw()
    raise RuntimeError("cloudwatch_client() is LOCAL_INFRA-only.")
