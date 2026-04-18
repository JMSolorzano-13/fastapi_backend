from collections.abc import Generator

import boto3
import pytest
from boto3_type_annotations.sqs import Client as SQSClient
from moto import mock_aws

from chalicelib.new.config.infra import envars


@pytest.fixture(autouse=True)
def sqs_client() -> Generator[SQSClient, None, None]:
    with mock_aws():
        client = boto3.client("sqs", region_name=envars.REGION_NAME)
        yield client


@pytest.fixture(autouse=True)
def mock_sqs_queue(sqs_client: SQSClient):
    sqs_client.create_queue(QueueName=envars.SQS_SEND_QUERY_METADATA)
    sqs_client.create_queue(QueueName=envars.SQS_COMPLETE_CFDIS)
    sqs_client.create_queue(QueueName=envars.SQS_SCRAP_ORCHESTRATOR)
