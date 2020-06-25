import pytest
from pydbtools.create_temp_table import check_sql

sql_pass1 = """
with x as (
SELECT temp__.c1, db.c2
FROM temp__
INNER JOIN db
ON temp__.c1 = db.c2)
select * from x
"""

sql_pass2 = """
SELECT temp__.c1, db.c2
FROM temp__
INNER JOIN db
ON temp__.c1 = db.c2
"""

sql_fail = """
create table x as (
SELECT temp__.c1, db.c2
FROM temp__
INNER JOIN db
ON temp__.c1 = db.c2)
select * from x
"""

@pytest.mark.parametrize(
    "test_input, expected",
    [
        (sql_pass1, True),
        (sql_pass2, True),
        (sql_fail, False)
    ],
)
def test_sql_parse(test_input, expected):
    try:
        check_sql(test_input)
        passed = True
    except ValueError as e:
        if str(e) == "The sql statement must be a select query":
            passed = False
        else:
            passed = "Unexpected fail reason"
    assert expected == passed


