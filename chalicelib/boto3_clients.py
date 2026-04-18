import json
import os
from functools import lru_cache

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
    """Return LocalStack endpoint URL if LOCAL_INFRA is enabled."""
    if os.environ.get("LOCAL_INFRA") == "1":
        return os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    return None


def lambda_client_pdf() -> LambdaClient:
    global _lambda_client_pdf
    if not _lambda_client_pdf:
        _lambda_client_pdf = boto3.client(
            "lambda",
            config=botocore.config.Config(
                # La invocación de la lambda de scraper toma más de los 60 segundos que por defecto soporta boto3  # noqa: E501
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
