import pytest
import os


# Get all SQL files in a dict
@pytest.fixture(scope="module")
def sql_dict():
    sql_dict = {}
    for fn in os.listdir("tests/data/"):
        if fn.endswith(".sql"):
            with open(os.path.join("tests/data/", fn)) as f:
                sql_dict[fn.split(".")[0]] = "".join(f.readlines())
    return sql_dict
