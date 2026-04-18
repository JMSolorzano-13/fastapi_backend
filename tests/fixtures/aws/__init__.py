import pytest
from moto import mock_aws


@pytest.fixture(scope="session", autouse=True)
def _moto_global_mock():
    yield mock_aws()


from tests.fixtures.aws.cognito import *  # noqa
from tests.fixtures.aws.s3 import *  # noqa
from tests.fixtures.aws.sqs import *  # noqa
