import pytest
import os
from moto import mock_s3
import boto3


# Get all SQL files in a dict
@pytest.fixture(scope="module")
def sql_dict():
    sql_dict = {}
    for fn in os.listdir("tests/data/"):
        if fn.endswith(".sql"):
            with open(os.path.join("tests/data/", fn)) as f:
                sql_dict[fn.split(".")[0]] = "".join(f.readlines())
    return sql_dict


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    mocked_envs = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
    ]
    for menv in mocked_envs:
        os.environ[menv] = "testing"

    yield  # Allows us to close down envs on exit

    for menv in mocked_envs:
        del os.environ[menv]


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.resource("s3", region_name="eu-west-1")


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-1")
