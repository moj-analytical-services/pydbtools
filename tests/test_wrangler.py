import pytest

from pydbtools._wrangler import init_athena_params


def mock_get_user_id_and_table_dir(
    boto3_session=None, force_ec2: bool = False, region_name: str = None
):
    if isinstance(boto3_session, dict) and "events" in boto3_session:
        boto3_session["events"].append("get_user_id_and_table_dir")
    return ("user_pytest", "s3://dummy/path/")


def mock_create_temp_database(
    temp_db_name: str = None,
    boto3_session=None,
    force_ec2: bool = False,
    region_name: str = None,
):
    if isinstance(boto3_session, dict) and "events" in boto3_session:
        boto3_session["events"].append(
            f"_create_temp_database({temp_db_name})"
        )
    return boto3_session


def get_empty_boto_log(*args, **kwargs):
    return {"name": "boto3_session", "events": []}


@init_athena_params
def fun_boto_only(boto3_session=None, *args, **kwargs):
    return locals()


@init_athena_params
def fun_with_sql(sql=None, boto3_session=None, *args, **kwargs):
    return locals()


@init_athena_params
def fun_with_sql_and_db_s3(
    sql=None, boto3_session=None, database=None, s3_output=None, **kwargs
):
    return locals()


@init_athena_params
def fun_with_sql_db_s3_ctas(
    sql=None,
    boto3_session=None,
    database=None,
    s3_output=None,
    ctas_approach=None,
    **kwargs,
):
    return locals()


# "sql, boto3_session, kwargs, expected_events, expect_warns",
@pytest.mark.parametrize(
    "fun_params, fun, expected_events, returned_db, expect_warns",
    [
        ({}, fun_boto_only, [], None, False),
        ({"boto3_session": "boto3_session"}, fun_boto_only, [], None, True),
        ({}, fun_with_sql, ["get_user_id_and_table_dir"], None, False),
        (
            {"sql": ""},
            fun_with_sql,
            ["get_user_id_and_table_dir"],
            None,
            False,
        ),
        (
            {},
            fun_with_sql_and_db_s3,
            ["get_user_id_and_table_dir"],
            None,
            False,
        ),
        (
            {"s3_output": "somewhere"},
            fun_with_sql_and_db_s3,
            ["get_user_id_and_table_dir"],
            None,
            True,
        ),
        (
            {"database": "__TEMP__"},
            fun_with_sql_and_db_s3,
            ["get_user_id_and_table_dir"],
            "mojap_de_temp_pytest",
            False,
        ),
        (
            {"database": "user_defined_db"},
            fun_with_sql_and_db_s3,
            ["get_user_id_and_table_dir"],
            "user_defined_db",
            False,
        ),
        (
            {"ctas_approach": True},
            fun_with_sql_db_s3_ctas,
            [
                "get_user_id_and_table_dir",
                "_create_temp_database(mojap_de_temp_pytest)",
            ],
            "mojap_de_temp_pytest",
            False,
        ),
        (
            {"ctas_approach": True, "database": "user_defined_db"},
            fun_with_sql_db_s3_ctas,
            ["get_user_id_and_table_dir"],
            "user_defined_db",
            False,
        ),
        (
            {"ctas_approach": False},
            fun_with_sql_db_s3_ctas,
            ["get_user_id_and_table_dir"],
            None,
            False,
        ),
        (
            {"s3_output": "somewhere", "ctas_approach": False},
            fun_with_sql_db_s3_ctas,
            ["get_user_id_and_table_dir"],
            None,
            True,
        ),
    ],
    ids=[
        "fun_boto_only",
        "fun_boto_only::boto3",
        "fun_with_sql",
        "fun_with_sql::sql",
        "fun_with_sql_and_db_s3",
        "fun_with_sql_and_db_s3::s3_output",
        "fun_with_sql_and_db_s3::database-temp",
        "fun_with_sql_and_db_s3::database-userdefined",
        "fun_with_sql_db_s3_ctas::ctas_approach-True",
        "fun_with_sql_db_s3_ctas::ctas_approach-True,database-userdefined",
        "fun_with_sql_db_s3_ctas::ctas_approach-False",
        "fun_with_sql_db_s3_ctas:s3_output",
    ],
)
def test_init_athena_params(
    fun_params, fun, expected_events, expect_warns, returned_db, monkeypatch
):
    monkeypatch.setattr(
        "pydbtools._wrangler.get_boto_session", get_empty_boto_log
    )
    monkeypatch.setattr(
        "pydbtools._wrangler.get_user_id_and_table_dir",
        mock_get_user_id_and_table_dir,
    )
    monkeypatch.setattr(
        "pydbtools._wrangler.get_database_name_from_userid",
        lambda user_id: "mojap_de_temp_pytest",
    )
    monkeypatch.setattr(
        "pydbtools._wrangler._create_temp_database", mock_create_temp_database
    )

    if expect_warns:
        with pytest.warns(UserWarning):
            out = fun(**fun_params)
    else:
        out = fun(**fun_params)

    assert out.get("boto3_session").get("events") == expected_events

    if fun.__name__ in ["fun_boto_only", "fun_with_sql"]:
        assert isinstance(out.get("boto3_session"), dict)

    elif fun.__name__ in ["fun_with_sql_and_db_s3", "fun_with_sql_db_s3_ctas"]:
        assert isinstance(out.get("boto3_session"), dict)
        assert out.get("s3_output") == "s3://dummy/path/"
        assert out["database"] == returned_db
        if fun.__name__ == "fun_with_sql_db_s3_ctas":
            assert out["ctas_approach"] == fun_params.get(
                "ctas_approach", False
            )
