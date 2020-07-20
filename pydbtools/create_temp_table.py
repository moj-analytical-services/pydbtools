import os
import sqlparse

from dataengineeringutils3.s3 import (
    delete_s3_folder_contents
)

from pydbtools.utils import (
    temp_database_name_prefix,
    get_user_id_and_table_dir,
    get_database_name_from_userid,
)

from pydbtools import get_athena_query_response

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


def add_user_to_temp(sql_query, username):
    """
    add alpha username to temp database name when user queries a table
    """
    return sql_query.lower().replace("from __temp__." f"from {username}.")


def create_database():
    """
    create athena database with temp name, pass if already exists
    """
    pass


def create_temp_table(
        sql_query: str,
        table_name: str,
        timeout: int = None,
        force_ec2: bool = False,
        region_name: str = "eu-west-1"):
    """
    Create a table inside the database from create database

    Args:
        sql_query (str):
            The SQL table you want to create a temp table out of. Should be a table that starts with a WITH or SELECT clause.
        
        table_name (str):
            The name of the temp table you wish to create
        
        force_ec2 (bool, optional):
            Boolean specifying if the user wants to force boto to get the
            credentials from the EC2. This is for dbtools which is the R wrapper that
            calls this package via reticulate and requires credentials to be refreshed
            via the EC2 instance (and therefore sets this to True) - this is not
            necessary when using this in Python. Default is False.. Defaults to False.
        
        region_name (str, optional):
            Name of the AWS region you want to run queries on. Defaults to "eu-west-1".
    """

    check_sql(sql_query)

    # Create named stuff
    user_id, out_path = get_user_id_and_table_dir(force_ec2, region_name)
    out_path = os.path.join(out_path, "__athena_temp_db__/", table_name)
    temp_db_name = get_database_name_from_userid(user_id)
    create_database(temp_db_name)

    # Clear out table every time
    delete_s3_folder_contents(out_path)

    ctas_query = f"""
    CREATE TABLE {temp_db_name}.{table_name}
        WITH (
            format = 'Parquet',
            orc_compression = 'SNAPPY',
            external_location = '{out_path}',
        )
    {sql_query}
    """

    _ = get_athena_query_response(
        sql_query=ctas_query,
        timeout=timeout,
        force_ec2=force_ec2,
        region_name=region_name,
    )
