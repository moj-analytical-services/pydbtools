import pytest
import toml


def test_set_module_values():
    import pydbtools as pydb

    assert pydb.utils.bucket == "mojap-athena-query-dump"
    assert pydb.utils.temp_database_name_prefix == "mojap_de_temp_"
    assert pydb.utils.aws_default_region == "eu-west-1"

    # Check changes work
    pydb.utils.bucket = "test"
    pydb.utils.temp_database_name_prefix = "test_"
    pydb.utils.aws_default_region = "eu-west-2"

    assert pydb.utils.bucket == "test"
    assert pydb.utils.temp_database_name_prefix == "test_"
    assert pydb.utils.aws_default_region == "eu-west-2"

    assert pydb.utils.get_database_name_from_userid("1234") == "test_1234"


def test_pyproject_toml_matches_version():
    import pydbtools as pydb

    with open("pyproject.toml") as f:
        proj = toml.load(f)
    assert pydb.__version__ == proj["project"]["version"]


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("abcde:12345", "12345"),
        ("abcde:my-name", None),
        ("d347875:alpha_user_some_user", "alpha_user_some_user"),
        ("d347875:alpha_user_some-user", "alpha_user_some_user"),
        ("d347875:alpha_app_some_user", "alpha_app_some_user"),
        ("d347875:alpha_app_some-user", "alpha_app_some_user"),
        ("abcde:my-name@digital.justice.gov.uk", "my_name"),
        ("abcde:my-name@justice.gov.uk", "my_name"),
        ("3f7f9e93-airflow_prod_role_name", "airflow_prod_role_name"),
        ("3f7f9e9-airflow_prod_role_name", "airflow_prod_role_name"),
        ("abcde:airflow_prod_role_name", "airflow_prod_role_name"),
        ("abcde:GitHubActions", "githubactions"),
        ("3f7f9e93-airflow-development-example", "airflow_development_example"),
    ],
)
def test_clean_user_id(test_input, expected):
    import pydbtools as pydb

    if expected is None:
        with pytest.raises(ValueError):
            pydb.utils.get_database_name_from_userid(test_input)

    else:
        assert (
            pydb.utils.get_database_name_from_userid(test_input)
            == f"{pydb.utils.temp_database_name_prefix}{expected}"
        )
