import os
import sqlparse

from dataengineeringutils3.s3 import delete_s3_folder_contents

from pydbtools.utils import (
    temp_database_name_prefix,
    get_user_id_and_table_dir,
    get_database_name_from_userid,
    clean_query,
)

from pydbtools.get_athena_query_response import get_athena_query_response


def check_sql(sql_query: str):
    """
    Validates sql_query to confirm it is a select statement
    """
    parsed = sqlparse.parse(clean_query(sql_query))
    i = 0
    for p in parsed:
        if p.get_type() != "SELECT" or i > 0:
            raise ValueError("The sql statement must be a single select query")
        i += 1


def create_temp_table(
    sql_query: str,
    table_name: str,
    timeout: int = None,
    force_ec2: bool = False,
    region_name: str = "eu-west-1",
):
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
            necessary when using this in Python. Default is False.
        
        region_name (str, optional):
            Name of the AWS region you want to run queries on. Defaults to "eu-west-1".
    """

    check_sql(sql_query)

    # Create named stuff
    user_id, out_path = get_user_id_and_table_dir(force_ec2, region_name)
    db_path = os.path.join(out_path, "__athena_temp_db__/")
    table_path = os.path.join(db_path, table_name)
    temp_db_name = get_database_name_from_userid(user_id)

    create_db_query = f"CREATE DATABASE IF NOT EXISTS {temp_db_name}"

    _ = get_athena_query_response(
        sql_query=create_db_query,
        timeout=None,
        force_ec2=force_ec2,
        region_name=region_name,
    )

    # Clear out table every time
    delete_s3_folder_contents(table_path)
    print(temp_db_name, table_name, out_path)
    ctas_query = f"""
    CREATE TABLE {temp_db_name}.{table_name}
        WITH (
            format = 'Parquet',
            parquet_compression  = 'SNAPPY',
            external_location = '{out_path}'
        )
    as {sql_query}
    """


    _ = get_athena_query_response(
        sql_query=ctas_query,
        timeout=timeout,
        force_ec2=force_ec2,
        region_name=region_name,
    )
