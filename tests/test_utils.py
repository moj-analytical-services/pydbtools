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

    assert pydb.utils.get_database_name_from_userid("bob") == "test_bob"


def test_pyproject_toml_matches_version():
    import pydbtools as pydb
    with open("pyproject.toml") as f:
        proj = toml.load(f)
    assert pydb.__version__ == proj["tool"]["poetry"]["version"]