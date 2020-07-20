import sqlparse
from pydbtools.utils import (
    get_database_name_from_userid,
    get_user_id_and_table_dir
)
from pydbtools.read_sql import read_sql

dbname = get_database_name_from_userid(get_user_id_and_table_dir()[0])

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
    return sql_query.lower().replace("from __temp__.", f"from {dbname}.")

def create_database():
    """
    create athena database with temp name, pass if already exists
    """  
    read_sql(f"create database if not exists {dbname}")

def create_temp_table(table_name):
    """
    create a table inside the database from create database
    """
    create_database()
    pass

