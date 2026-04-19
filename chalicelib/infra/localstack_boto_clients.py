"""AWS SDK (boto3) — **only module** that imports ``boto3`` for app/scripts/tests.

- **Singletons** (``s3_client``, ``sqs_client``, …): LocalStack when ``LOCAL_INFRA=1``.
- **Factories** (``make_ephemeral_*``): SQS/S3/Cognito for ``queue_transport``, workers, ``moto``.

Cloud: ``azure_blob_s3_shim`` + Service Bus; ``chalicelib.boto3_clients`` routes there.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any

import boto3
import botocore
from boto3_type_annotations.cloudwatch import Client as CloudWatchClient
from boto3_type_annotations.cognito_idp import Client as CognitoClient
from boto3_type_annotations.lambda_ import Client as LambdaClient
from boto3_type_annotations.s3 import Client as S3Client
from boto3_type_annotations.secretsmanager import Client as SecretsManagerClient
from boto3_type_annotations.ses import Client as SESClient
from boto3_type_annotations.sqs import Client as SQSClient

from chalicelib.new.config.infra import envars

_s3_client: S3Client | None = None
_sqs_client: SQSClient | None = None
_ses_client: SESClient | None = None
_secretsmanager_client: SecretsManagerClient | None = None
_lambda_client: LambdaClient | None = None
_cognito_client: CognitoClient | None = None
_cloudwatch_client: CloudWatchClient | None = None
_lambda_client_pdf: LambdaClient | None = None


def _get_endpoint_url() -> str | None:
    return os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")


def lambda_client_pdf() -> LambdaClient:
    global _lambda_client_pdf
    if not _lambda_client_pdf:
        _lambda_client_pdf = boto3.client(
            "lambda",
            config=botocore.config.Config(
                read_timeout=900,
                connect_timeout=900,
                retries={"max_attempts": 0},
            ),
            region_name=envars.REGION_NAME,
            endpoint_url=_get_endpoint_url(),
        )
    assert _lambda_client_pdf
    return _lambda_client_pdf


def s3_client() -> S3Client:
    global _s3_client
    if not _s3_client:
        _s3_client = boto3.client(
            "s3",
            aws_access_key_id=envars.S3_ACCESS_KEY,
            aws_secret_access_key=envars.S3_SECRET_KEY,
            region_name=envars.REGION_NAME,
            endpoint_url=_get_endpoint_url(),
        )
    assert _s3_client
    return _s3_client


def sqs_client() -> SQSClient:
    global _sqs_client
    if not _sqs_client:
        _sqs_client = boto3.client(
            "sqs",
            region_name=envars.REGION_NAME,
            endpoint_url=_get_endpoint_url(),
        )
    assert _sqs_client
    return _sqs_client


def ses_client() -> SESClient:
    global _ses_client
    if not _ses_client:
        _ses_client = boto3.client(
            "ses",
            region_name=envars.REGION_NAME,
            endpoint_url=_get_endpoint_url(),
        )
    assert _ses_client
    return _ses_client


def secretsmanager_client() -> SecretsManagerClient:
    global _secretsmanager_client
    if not _secretsmanager_client:
        _secretsmanager_client = boto3.client(
            "secretsmanager",
            region_name=envars.REGION_NAME,
            endpoint_url=_get_endpoint_url(),
        )
    assert _secretsmanager_client
    return _secretsmanager_client


@lru_cache
def secretsmanager_get(secret_id: str) -> dict:
    return json.loads(secretsmanager_client().get_secret_value(SecretId=secret_id)["SecretString"])


def lambda_client() -> LambdaClient:
    global _lambda_client
    if not _lambda_client:
        _lambda_client = boto3.client(
            "lambda",
            region_name=envars.REGION_NAME,
            endpoint_url=_get_endpoint_url(),
        )
    assert _lambda_client
    return _lambda_client


def cognito_client() -> CognitoClient:
    global _cognito_client
    if not _cognito_client:
        _cognito_client = boto3.client(
            "cognito-idp",
            region_name=envars.REGION_NAME,
            endpoint_url=_get_endpoint_url(),
        )
    assert _cognito_client
    return _cognito_client


def cloudwatch_client() -> CloudWatchClient:
    global _cloudwatch_client
    if not _cloudwatch_client:
        _cloudwatch_client = boto3.client(
            "cloudwatch",
            region_name=envars.REGION_NAME,
            endpoint_url=_get_endpoint_url(),
        )
    assert _cloudwatch_client
    return _cloudwatch_client


def make_ephemeral_sqs_client(
    *,
    region_name: str | None = None,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> SQSClient:
    """Construct an SQS client (LocalStack, moto tests, or explicit keys)."""
    kwargs: dict[str, Any] = {"region_name": region_name or envars.REGION_NAME}
    if endpoint_url is not None:
        kwargs["endpoint_url"] = endpoint_url
    if aws_access_key_id is not None:
        kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key is not None:
        kwargs["aws_secret_access_key"] = aws_secret_access_key
    return boto3.client("sqs", **kwargs)


def make_ephemeral_sqs_client_from_transport_env() -> SQSClient:
    """SQS client for ``queue_transport`` / operator scripts (env-driven endpoint + keys)."""
    endpoint: str | None = None
    if os.environ.get("LOCAL_INFRA") == "1":
        endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    elif os.environ.get("AWS_ENDPOINT_URL"):
        endpoint = os.environ["AWS_ENDPOINT_URL"]
    ak = os.environ.get("AWS_ACCESS_KEY_ID")
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
    return make_ephemeral_sqs_client(
        endpoint_url=endpoint,
        aws_access_key_id=ak or None,
        aws_secret_access_key=sk or None,
    )


def make_ephemeral_s3_client(
    *,
    region_name: str | None = None,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> S3Client:
    """Construct an S3 client (LocalStack, moto, or explicit keys)."""
    kwargs: dict[str, Any] = {
        "region_name": region_name or envars.REGION_NAME,
        "aws_access_key_id": aws_access_key_id or envars.S3_ACCESS_KEY,
        "aws_secret_access_key": aws_secret_access_key or envars.S3_SECRET_KEY,
    }
    if endpoint_url is not None:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)


def make_ephemeral_cognito_client(
    *,
    region_name: str | None = None,
    endpoint_url: str | None = None,
) -> CognitoClient:
    """Construct a Cognito IDP client (moto tests / LocalStack)."""
    kwargs: dict[str, Any] = {"region_name": region_name or envars.REGION_NAME}
    if endpoint_url is not None:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("cognito-idp", **kwargs)
