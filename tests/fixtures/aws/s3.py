import pytest

from chalicelib.boto3_clients import s3_client
from chalicelib.new.config.infra import envars


@pytest.fixture(autouse=True)
def mock_s3_bucket():
    s3_client().create_bucket(Bucket=envars.S3_CERTS)
    s3_client().create_bucket(Bucket=envars.S3_EXPORT)
    s3_client().create_bucket(Bucket=envars.S3_UUIDS_COMPARE_SCRAPER)
