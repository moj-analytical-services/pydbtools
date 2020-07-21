import pytest
from pydbtools.create_temp_table import check_sql
from pydbtools.utils import replace_temp_database_name_reference

sql1 = """
with x as (SELECT __TEMP__.y.c1, db.tb.c2
FROM __TEMP__.y
INNER JOIN db.tb
ON __TEMP__.y.c1 = db.tb.c2)
select * from x;
"""

sql2 = """
SELECT y.c1, db.c2
FROM __temp__.y as y
INNER JOIN db
ON y.c1 = db.c2
where y.c1 == 'Billy'
"""

sql3 = """
create table x as (
SELECT __temp__.y.c1, tb.c2
FROM __temp__.y
INNER JOIN tb
ON __temp__.y.c1 = tb.c2)
select * from x
"""

sql4 = """
SELECT y.c1, db.c2
FROM __temp__.y as y
INNER JOIN db
ON y.c1 = db.c2
where y.c1 == 'Billy';
SELECT y.c1, db.c2
FROM __temp__.y as y
INNER JOIN db
ON y.c1 = db.c2
where y.c1 == 'Billy'
"""


@pytest.mark.parametrize(
    "test_input, expected", [(sql1, True), (sql2, True), (sql3, False), (sql4, False)],
)
def test_sql_parse(test_input: str, expected: bool):
    try:
        check_sql(test_input)
        passed = True
    except ValueError as e:
        if str(e) == "The sql statement must be a single select query":
            passed = False
        else:
            passed = "Unexpected fail reason"
    assert expected == passed


@pytest.mark.parametrize(
    "test_input, expected", 
    [(sql1, " ".join(sql1.splitlines()).strip().replace("__TEMP__", "dbname")), 
    (sql2, " ".join(sql2.splitlines()).strip().replace("__temp__", "dbname")+";"),
    (sql3, " ".join(sql3.splitlines()).strip().replace("__temp__", "dbname")+";"), 
    (sql4, " ".join(sql4.splitlines()).strip().replace("__temp__", "dbname")+";")]
)
def test_replace_temp_database_name_reference(test_input: str, expected: bool):
    sql = replace_temp_database_name_reference(test_input, "dbname")
    assert sql == expected
    
