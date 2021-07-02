import pydbtools as pydb

def test_basic_read_sql_file():

    expected = (
        "SELECT *\n"
        "FROM db.table\n"
        "WHERE something = 'something else'\n"
    )
    actual = pydb.read_sql_file(
        "tests/data/basic.sql"
    )
    assert expected == actual

    actual = pydb.read_sql_file(
        "tests/data/basic.sql", {}
    )
    assert expected == actual


def test_jinja_read_sql_file():
    expected = (
        "SELECT *\n"
        "FROM bob.a_table\n"
        "WHERE category IN (0,1,2)"
    )
    args = {
        "db_name": "bob",
        "table_name": "a_table",
        "values": [0,1,2]
    }

    actual = pydb.read_sql_file(
        "tests/data/templated.sql",
        jinja_args=args
    )
    assert expected == actual
