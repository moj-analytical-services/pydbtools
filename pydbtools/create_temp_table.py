import sqlparse


def check_sql(sql_query: str):
    """
    Validates sql_query to confirm it is a select statement
    """
    sql_query = sql_query.rstrip()
    if sql_query[-1] == ";":
        sql_query = sql_query.rstrip(";")
    parsed = sqlparse.parse(sql_query)
    for p in parsed:
        if p.get_type() != "SELECT":
            raise ValueError("The sql statement must be a select query")


def add_user_to_temp(sql_query):
    """
    add alpha username to temp database name when user queries a table
    """
    return sql_query.lower().replace("from __temp__." f"from {username}.")