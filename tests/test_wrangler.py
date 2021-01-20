import os

import boto3
from moto import mock_s3
import pytest

from pydbtools.wrangler import (
    init_athena_params,
    get_boto_session,
)

def mock_get_user_id_and_table_dir(
    boto3_session=None, force_ec2: bool = False, region_name: str = "eu-west-1"
):
    if (
            isinstance(boto3_session, dict) and
            "events" in boto3_session
        ):
        boto3_session["events"].append("get_user_id_and_table_dir")
    return ("user_pytest", "s3://dummy/path/")


def mock_create_temp_database(
    temp_db_name: str = None,
    boto3_session=None,
    force_ec2: bool = False,
    region_name: str = "eu-west-1",
):
    if (
        isinstance(boto3_session, dict) and
        "events" in boto3_session
    ):
        boto3_session["events"].append("_create_temp_database")
    return boto3_session


def get_empty_boto_log(*args, **kwargs):
    return {"name": "boto3_session", "events": []}

@init_athena_params
def fun_boto_only(
    boto3_session=None,
    *args,
    **kwargs
):
    return locals()


@init_athena_params
def fun_with_sql(
    sql="",
    boto3_session=None,
    *args,
    **kwargs
):
    return locals()

@init_athena_params
def fun_with_sql_and_db_s3(
    sql="",
    boto3_session=None,
    database=None,
    s3_output=None,
    **kwargs
):
    return locals()

@init_athena_params
def fun_with_db_s3(
    boto3_session=None,
    database=None,
    s3_output=None,
    **kwargs
):
    return locals()


# "sql, boto3_session, kwargs, expected_events, expect_warns",
@pytest.mark.parametrize(
"fun_params, fun, expected_events, expect_warns",
[
    ({}, fun_boto_only, [], False),
    ({"boto3_session": "boto3_session"}, fun_boto_only, [], True),
    ({}, fun_with_sql, ["get_user_id_and_table_dir"], False),
    ({"sql": ""}, fun_with_sql, ["get_user_id_and_table_dir"], False),
    ({}, fun_with_sql_and_db_s3, ["get_user_id_and_table_dir", "_create_temp_database"], False),
    ({"s3_output": "somewhere"}, fun_with_sql_and_db_s3, ["get_user_id_and_table_dir", "_create_temp_database"], True),
],
ids=[
    "fun_boto_only",
    "fun_boto_only::boto3",
    "fun_with_sql",
    "fun_with_sql::sql",
    "fun_with_sql_and_db_s3",
    "fun_with_sql_and_db_s3::s3_output"
]
)
def test_init_athena_params(fun_params, fun, expected_events, expect_warns, monkeypatch):
    monkeypatch.setattr(
        "pydbtools.wrangler.get_boto_session",
        get_empty_boto_log
    ) 
    monkeypatch.setattr(
        "pydbtools.wrangler.get_user_id_and_table_dir",
        mock_get_user_id_and_table_dir
    ) 
    monkeypatch.setattr(
        "pydbtools.wrangler.get_database_name_from_userid",
        lambda user_id: "mojap_de_temp_pytest"
    )
    monkeypatch.setattr(
        "pydbtools.wrangler._create_temp_database",
        lambda user_id: mojap_de_temp_pytest
    )

    if expect_warns:
        with pytest.warns(UserWarning):
            out = fun(**fun_params)
    else:
        out = fun(**fun_params)

    assert out.get("boto3_session").get("events") == expected_events

    if fun.__name__ == "":
        pass