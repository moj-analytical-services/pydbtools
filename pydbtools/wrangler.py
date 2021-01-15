import awswrangler as wr
import awswrangler.athena as ath
import os
import sqlparse
import warnings

import inspect
import functools

from pydbtools.utils import (
    temp_database_name_prefix,
    get_user_id_and_table_dir,
    get_database_name_from_userid,
    clean_query,
    get_default_args,
    get_boto_session,
    replace_temp_database_name_reference,
)

# Wrapper used to set parameters in the athena wrangler functions
# before they are called
def init_athena_params(func):
    """
    Takes a wrangler athena function and sets the following:
    boto3_session and s3_output_path if exists in function param.

    Args:
        func (Callable): An function from wr.athena that requires
        boto3_session. If the func has an s3_output this is also 
        standardised.

    Returns:
        Similar function call but with pre-defined params.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get the boto3 session
        setup_defaults = get_default_args(get_boto_session)
        setup_kwargs = {}
        for k, v in setup_defaults.items():
            setup_kwargs[k] = kwargs.pop(k, v)
        boto3_session = get_boto_session(**setup_kwargs)

        # Get parameters from function and overwrite specific params
        sig = inspect.signature(func)
        argmap = sig.bind_partial(*args, **kwargs).arguments

        if "boto3_session" in argmap:
            warn_msg = (
                "Warning parameter 'boto3_session' cannot be set. "
                "Is defined by setting 'force_ec2' and 'region' params. "
                "(Input boto3_session will be ignored)."
            )
            warnings.warn(warn_msg)
        argmap["boto3_session"] = boto3_session

        # Check SQL for __temp__ keyword references and set s3 table path
        if ("s3_output" in argmap) or ("sql" in argmap):
            user_id, s3_output = get_user_id_and_table_dir(boto3_session)
        
        # Set s3_output to predefined path otherwise skip
        if "s3_output" in sig.parameters:
            if "s3_output" in argmap:
                warn_msg = (
                    "Warning parameter 's3_output' cannot be set. "
                    "Is automatically generated (input ignored)."
                )
                warnings.warn(warn_msg)
            
            # Set s3 to default s3 path
            argmap["s3_output"] = s3_output

        # Fix sql before it is passed to athena
        if "sql" in argmap:
            temp_db_name = get_database_name_from_userid(user_id)
            argmap["sql"] = replace_temp_database_name_reference(
                argmap["sql"],
                temp_db_name
            )

        # Set database to None when not needed
        if (
            "database" in sig.parameters and
            (argmap.get("database", "__temp__").lower() == "__temp__")
        ):
            if (
                "ctas_approach" in sig.parameters and 
                argmap.get("ctas_approach", True)
            ):
                argmap["database"] = temp_db_name
            else:
                argmap["database"] = None

        return func(**argmap)
    return wrapper


# Override all existing awswrangler.athena functions for pydbtools
read_sql_query = init_athena_params(ath.read_sql_query)
read_sql_table = init_athena_params(ath.read_sql_table)
create_athena_bucket = init_athena_params(ath.create_athena_bucket)
describe_table = init_athena_params(ath.describe_table)
get_query_columns_types = init_athena_params(ath.get_query_columns_types)
get_query_execution = init_athena_params(ath.get_query_execution)
get_work_group = init_athena_params(ath.get_work_group)
repair_table = init_athena_params(ath.repair_table)
show_create_table = init_athena_params(ath.show_create_table)
start_query_execution = init_athena_params(ath.start_query_execution)
stop_query_execution = init_athena_params(ath.stop_query_execution)
wait_query = init_athena_params(ath.wait_query)


def start_query_execution_and_wait(sql, *args, **kwargs):
    """Calls start_query_execution followed by wait_query.
    *args and **kwargs are passed to start_query_execution

    Args:
        sql (str): An SQL string. Which works with __TEMP__ references.
    """

    query_execution_id = start_query_execution(sql, *args, **kwargs)
    return wait_query(query_execution_id)


def check_sql(sql: str):
    """
    Validates sql to confirm it is a select statement
    """
    parsed = sqlparse.parse(clean_query(sql))
    i = 0
    for p in parsed:
        if p.get_type() != "SELECT" or i > 0:
            raise ValueError("The sql statement must be a single select query")
        i += 1


def create_temp_database(
    temp_db_name:str = None,
    force_ec2: bool = False,
    region_name: str = "eu-west-1"
):
    if temp_db_name is None:
        user_id, _ = get_user_id_and_table_dir(
            boto3_session=None,
            force_ec2=force_ec2,
            region_name=region_name
        )
        temp_db_name = get_database_name_from_userid(user_id)

    create_db_query = f"CREATE DATABASE IF NOT EXISTS {temp_db_name}"

    return start_query_execution_and_wait(
        sql=create_db_query,
        force_ec2=force_ec2,
        region_name=region_name,
    )


def create_temp_table(
    sql: str,
    table_name: str,
    force_ec2: bool = False,
    region_name: str = "eu-west-1",
):
    """
    Create a table inside the database from create database

    Args:
        sql (str):
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

    check_sql(sql)

    # Create named stuff
    user_id, out_path = get_user_id_and_table_dir(
        boto3_session=None,
        force_ec2=force_ec2,
        region_name=region_name
    )
    db_path = os.path.join(out_path, "__athena_temp_db__/")
    table_path = os.path.join(db_path, table_name)
    temp_db_name = get_database_name_from_userid(user_id)

    _ = create_temp_database(
        temp_db_name,
        force_ec2=force_ec2,
        region_name=region_name
    )

    # Clear out table every time
    wr.s3.delete_objects(table_path)

    drop_table_query = f"DROP TABLE IF EXISTS {temp_db_name}.{table_name}"

    _ = start_query_execution_and_wait(
        sql=drop_table_query,
        force_ec2=force_ec2,
        region_name=region_name,
    )

    ctas_query = f"""
    CREATE TABLE {temp_db_name}.{table_name}
        WITH (
            format = 'Parquet',
            parquet_compression  = 'SNAPPY',
            external_location = '{table_path}'
        )
    as {sql}
    """

    _ = start_query_execution_and_wait(
        sql=ctas_query,
        force_ec2=force_ec2,
        region_name=region_name,
    )
