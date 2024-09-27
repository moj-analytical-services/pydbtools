import awswrangler as wr
import awswrangler.athena as ath
import os
import sqlparse
import warnings
import logging
import pprint
import pandas as pd
import re
from typing import Iterator, Optional, List
import time
import inspect
import functools
from arrow_pd_parser import reader

from pydbtools.utils import (
    get_user_id_and_table_dir,
    get_database_name_from_userid,
    get_database_name_from_sql,
    clean_query,
    get_default_args,
    get_boto_session,
    replace_temp_database_name_reference,
    _set_region_name,
    s3_path_join,
    get_table_location,
)


logger = logging.getLogger(__name__)


# Wrapper used to set parameters in the athena wrangler functions
# before they are called
def init_athena_params(func=None, *, allow_boto3_session=False):  # noqa: C901
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
    # Allows parameterisation of this wrapper fun
    if func is None:
        return functools.partial(
            init_athena_params, allow_boto3_session=allow_boto3_session
        )

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get parameters from function and overwrite specific params
        sig = inspect.signature(func)
        argmap = sig.bind_partial(*args, **kwargs).arguments

        # Create a db flag
        database_flag = "database" in sig.parameters and (
            argmap.get("database", "__temp__") in ["__temp__", "__TEMP__"]
        )

        # If wrapper allows boto3 session being defined by user
        # and it has been then do not create new boto3 session
        # otherwise do
        if allow_boto3_session and argmap.get("boto3_session"):
            pass
        else:
            # Get the boto3 session
            setup_defaults = get_default_args(get_boto_session)
            setup_kwargs = {}
            for k, v in setup_defaults.items():
                setup_kwargs[k] = kwargs.pop(k, v)
            boto3_session = get_boto_session(**setup_kwargs)

            if argmap.get("boto3_session") is not None:
                warn_msg = (
                    "Warning parameter 'boto3_session' cannot be set. "
                    "Is defined by setting 'force_ec2' and 'region' params. "
                    "(Input boto3_session will be ignored)."
                )
                warnings.warn(warn_msg)
            argmap["boto3_session"] = boto3_session

        # Set s3 table path and get temp_db_name
        if (
            ("s3_output" in sig.parameters)
            or ("sql" in sig.parameters)
            or database_flag
        ):
            user_id, s3_output = get_user_id_and_table_dir(boto3_session)
            temp_db_name = get_database_name_from_userid(user_id)

        # Set s3_output to predefined path otherwise skip
        if "s3_output" in sig.parameters:
            if argmap.get("s3_output") is not None:
                warn_msg = (
                    "Warning parameter 's3_output' cannot be set. "
                    "Is automatically generated (input ignored)."
                )
                warnings.warn(warn_msg)

            # Set s3 to default s3 path
            argmap["s3_output"] = s3_output

        # Set ctas_approach to True if not set.
        # Although awswrangler does this by default, we want to ensure
        # that timestamps are read in correctly to pandas using pyarrow.
        # Therefore forcing the default option to be True in case future
        # versions of wrangler change their default behaviour.
        if "ctas_approach" in sig.parameters and argmap.get("ctas_approach") is None:
            argmap["ctas_approach"] = True

        # Set database to None or set to keyword temp when not needed
        if database_flag:
            if "ctas_approach" in sig.parameters and argmap["ctas_approach"]:
                argmap["database"] = temp_db_name
                _ = _create_temp_database(temp_db_name, boto3_session=boto3_session)
            elif argmap.get("database", "").lower() == "__temp__":
                argmap["database"] = temp_db_name
            else:
                argmap["database"] = None

        # Fix sql before it is passed to athena
        if "sql" in argmap:
            argmap["sql"] = replace_temp_database_name_reference(
                argmap["sql"], temp_db_name
            )

        if (
            "sql" in sig.parameters
            and "database" in sig.parameters
            and argmap.get("database") is None
        ):
            argmap["database"] = get_database_name_from_sql(argmap.get("sql", ""))

        # Set pyarrow_additional_kwargs
        if (
            "pyarrow_additional_kwargs" in argmap
            and argmap.get("pyarrow_additional_kwargs", None) is None
        ):
            argmap["pyarrow_additional_kwargs"] = {
                "coerce_int96_timestamp_unit": "ms",
                "timestamp_as_object": True,
            }

        logger.debug(f"Modifying function {func.__name__}")
        logger.debug(pprint.pformat(dict(argmap)))
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
tables = init_athena_params(wr.catalog.tables)
create_ctas_table = init_athena_params(ath.create_ctas_table, allow_boto3_session=True)


@init_athena_params
def start_query_execution_and_wait(sql, *args, **kwargs):
    """Calls start_query_execution followed by wait_query.
    *args and **kwargs are passed to start_query_execution

    Args:
        sql (str): An SQL string. Which works with __TEMP__ references.
    """

    # Function wrapper is applied to top of function so we need
    # to call the original unwrapped athena fun to ensure the wrapper fun
    # is not called again
    query_execution_id = ath.start_query_execution(sql, *args, **kwargs)
    return ath.wait_query(query_execution_id, boto3_session=kwargs.get("boto3_session"))


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


# This is not necessary atm but incase future changes are made
#  I think it is better to create "public" and "private" method
# where the public function is wrapped by init_athena_params
# this wrapper also calls the private function to avoid the wrapper
# calling itself
@init_athena_params(allow_boto3_session=True)
def create_temp_database(
    temp_db_name: str = None,
    boto3_session=None,
    force_ec2: bool = False,
    region_name: str = None,
):
    region_name = _set_region_name(region_name)
    _ = _create_temp_database(
        temp_db_name=temp_db_name,
        boto3_session=boto3_session,
        force_ec2=force_ec2,
        region_name=region_name,
    )


def _create_temp_database(
    temp_db_name: str = None,
    boto3_session=None,
    force_ec2: bool = False,
    region_name: str = None,
):
    region_name = _set_region_name(region_name)

    user_id, s3_output = get_user_id_and_table_dir(
        boto3_session=boto3_session,
        force_ec2=force_ec2,
        region_name=region_name,
    )

    if temp_db_name is None or temp_db_name.lower().strip() == "__temp__":
        temp_db_name = get_database_name_from_userid(user_id)

    create_db_query = f"CREATE DATABASE IF NOT EXISTS {temp_db_name}"

    q_e_id = ath.start_query_execution(
        create_db_query,
        s3_output=s3_output,
        boto3_session=boto3_session,
    )

    return ath.wait_query(q_e_id, boto3_session=boto3_session)


@init_athena_params
def create_temp_table(
    sql: str,
    table_name: str,
    boto3_session=None,
    force_ec2: bool = False,
    region_name: str = None,
):
    """
    Create a table inside the temporary database from create table

    Args:
        sql (str):
            The SQL table you want to create a temp table out of. Should
            be a table that starts with a WITH or SELECT clause.

        table_name (str):
            The name of the temp table you wish to create

        force_ec2 (bool, optional):
            Boolean specifying if the user wants to force boto to get the
            credentials from the EC2. This is for dbtools which is the R
            wrapper that calls this package via reticulate and requires
            credentials to be refreshed via the EC2 instance (and
            therefore sets this to True) - this is not
            necessary when using this in Python. Default is False.

        region_name (str, optional):
            Name of the AWS region you want to run queries on. Defaults to
            pydbtools.utils.aws_default_region (which if left unset is
            "eu-west-1").
    """
    region_name = _set_region_name(region_name)
    check_sql(sql)

    # Create named stuff
    user_id, out_path = get_user_id_and_table_dir(boto3_session=boto3_session)
    db_path = os.path.join(out_path, "__athena_temp_db__/")
    # Include timestamp in path to avoid permissions problems with
    # previous sessions
    ts = str(time.time()).replace(".", "")
    table_path = os.path.join(db_path, ts, table_name)
    temp_db_name = get_database_name_from_userid(user_id)

    _ = create_temp_database(temp_db_name, boto3_session=boto3_session)

    # Clear out table every time, making sure other tables aren't being
    # cleared out
    delete_temp_table(table_name, boto3_session=boto3_session)

    ctas_query = f"""
    CREATE TABLE {temp_db_name}.{table_name}
        WITH (
            format = 'Parquet',
            parquet_compression  = 'SNAPPY',
            external_location = '{table_path}'
        )
    as {sql}
    """

    q_e_id = ath.start_query_execution(ctas_query, boto3_session=boto3_session)

    ath.wait_query(q_e_id, boto3_session=boto3_session)


def create_table(
    sql: str,
    database: str,
    table: str,
    location: str,
    partition_cols: Optional[List[str]] = None,
    boto3_session=None,
):
    """
    Create a table in a database from a SELECT statement

    Args:
        sql (str): SQL starting with a WITH or SELECT clause
        database (str): Database name
        table (str): Table name
        location (str): S3 path to where the table should be stored
        partition_cols (List[str]): partition columns (optional)
        boto3_session: optional boto3 session
    """
    return ath.create_ctas_table(
        sql=sql,
        database=database,
        ctas_database=database,
        ctas_table=table,
        s3_output=s3_path_join(location, table + ".parquet"),
        partitioning_info=partition_cols,
        wait=True,
        boto3_session=boto3_session,
    )


def _create_temp_table_in_sql(sql: str) -> bool:
    """
    Allows the user to write SQL of the format
    CREATE TEMP TABLE tablename AS (...)

    Args:
        sql (str):
            An SQL query.

    Returns:
        A bool indicating whether a temporary table was
        created (True) or whether the SQL still needs to
        be processed (False).
    """

    sql = clean_query(sql, fmt_opts={"strip_comments": True})
    m = re.fullmatch(
        r"create\s+temp\s+table\s+(\S+)\s+as\s+(.*)", sql, flags=re.IGNORECASE
    )
    if m:
        table_name = m.group(1)
        table_sql = m.group(2)

        # Remove parentheses from the SQL
        m = re.fullmatch(r"\((.*)\)", table_sql)
        if m:
            table_sql = m.group(1)

        create_temp_table(table_sql, table_name)
        return True
    else:
        return False


def read_sql_queries(sql: str) -> Optional[pd.DataFrame]:
    """
    Reads a number of SQL statements and returns the result of
    the last select statement as a dataframe.
    Temporary tables can be created using
    CREATE TEMP TABLE tablename AS (sql query)
    and accessed using __temp__ as the database.

    Args:
        sql (str): SQL commands

    Returns:
        An iterator of Pandas DataFrames.

    Example:
        If the file eg.sql contains the SQL code
            create temp table A as (
                select * from database.table1
                where year = 2021
            );

            create temp table B as (
                select * from database.table2
                where amount > 10
            );

            select * from __temp__.A
            left join __temp__.B
            on A.id = B.id;

        df = read_sql_queries(open('eg.sql', 'r').read())
    """

    df = None
    for df in read_sql_queries_gen(sql):
        pass
    return df


def read_sql_queries_gen(sql: str) -> Iterator[pd.DataFrame]:
    """
    Reads a number of SQL statements and returns the result of
    any select statements as a dataframe generator.
    Temporary tables can be created using
    CREATE TEMP TABLE tablename AS (sql query)
    and accessed using __temp__ as the database.

    Args:
        sql (str): SQL commands

    Returns:
        An iterator of Pandas DataFrames.

    Example:
        If the file eg.sql contains the SQL code
            create temp table A as (
                select * from database.table1
                where year = 2021
            );

            create temp table B as (
                select * from database.table2
                where amount > 10
            );

            select * from __temp__.A
            left join __temp__.B
            on A.id = B.id;

            select * from __temp__.A
            where country = 'UK'

        df_iter = read_sql_queries(open('eg.sql', 'r').read())
        df1 = next(df_iter)
        df2 = next(df_iter)
    """

    for query in sqlparse.parse(sql):
        if not _create_temp_table_in_sql(str(query)):
            if query.get_type() == "SELECT":
                yield read_sql_query(str(query))
            else:
                start_query_execution_and_wait(str(query))


@init_athena_params(allow_boto3_session=True)
def delete_table_and_data(table: str, database: str, boto3_session=None):
    """
    Deletes both a table from an Athena database and the underlying data on S3.

    Args:
        table (str): The table name to drop.
        database (str): The database name.

    Returns:
        True if table exists and is deleted, False if table
        does not exist
    """

    if table in list(tables(database=database, limit=None)["Table"]):
        path = get_table_location(
            database=database, table=table, boto3_session=boto3_session
        )
        wr.s3.delete_objects(path, boto3_session=boto3_session)
        wr.catalog.delete_table_if_exists(
            database=database, table=table, boto3_session=boto3_session
        )
        return True
    else:
        return False


@init_athena_params(allow_boto3_session=True)
def delete_temp_table(table: str, boto3_session=None):
    """
    Deletes a temporary table.

    Args:
        table (str): The table name to drop.

    Returns:
        True if table exists and is deleted, False if table
        does not exist
    """

    user_id, table_dir = get_user_id_and_table_dir(boto3_session=boto3_session)
    database = get_database_name_from_userid(user_id)
    _create_temp_database(database, boto3_session=boto3_session)

    if table in list(tables(database=database, limit=None)["Table"]):
        path = get_table_location(
            database=database, table=table, boto3_session=boto3_session
        )

        # Use try in case table was set up in previous session
        try:
            wr.s3.delete_objects(path, boto3_session=boto3_session)
        except wr.exceptions.ServiceApiError:
            pass

        wr.catalog.delete_table_if_exists(
            database=database, table=table, boto3_session=boto3_session
        )
        return True
    else:
        return False


@init_athena_params(allow_boto3_session=True)
def delete_database_and_data(database: str, boto3_session=None):
    """
    Deletes both an Athena database and the underlying data on S3.

    Args:
        database (str): The database name to drop.

    Returns:
        True if database exists and is deleted, False if database
        does not exist
    """
    if database not in (db["Name"] for db in wr.catalog.get_databases()):
        return False
    for table in wr.catalog.get_tables(database=database, boto3_session=boto3_session):
        delete_table_and_data(table["Name"], database, boto3_session=boto3_session)
    wr.catalog.delete_database(database, boto3_session=boto3_session)
    return True


@init_athena_params(allow_boto3_session=True)
def delete_partitions_and_data(
    database: str, table: str, expression: str, boto3_session=None
):
    """
    Deletes partitions and the underlying data on S3 from an Athena
    database table matching an expression.

    Args:
        database (str): The database name.
        table (str): The table name.
        expression (str): The expression to match.

    Please see
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_partitions # noqa
    for instructions on the expression construction, but at a basic level
    you can use SQL syntax on your partition columns.

    Examples:
    delete_partitions_and_data("my_database", "my_table", "year = 2020 and month = 5")
    """

    matched_partitions = wr.catalog.get_partitions(
        database, table, expression=expression, boto3_session=boto3_session
    )
    # Delete data at partition locations
    for location in matched_partitions:
        wr.s3.delete_objects(location, boto3_session=boto3_session)
    # Delete partitions
    wr.catalog.delete_partitions(
        table,
        database,
        list(matched_partitions.values()),
        boto3_session=boto3_session,
    )


def save_query_to_parquet(sql: str, file_path: str) -> None:
    """
    Saves the results of a query to a parquet file
    at a given location.

    Args:
        sql (str): The SQL query.
        file_path (str): The path to save the result to.

    Examples:
    save_query_to_parquet(
        "select * from my database.my_table",
        "result.parquet"
    )
    """

    df = read_sql_query(sql)
    df.to_parquet(file_path)

    return None


@init_athena_params(allow_boto3_session=True)
def dataframe_to_temp_table(df: pd.DataFrame, table: str, boto3_session=None) -> None:
    """
    Creates a temporary table from a dataframe.

    Args:
        df (pandas.DataFrame): A pandas DataFrame
        table (str): The name of the table in the temporary database
        boto3_session: opeional boto3 sesssion
    """
    user_id, table_dir = get_user_id_and_table_dir(boto3_session=boto3_session)
    db = get_database_name_from_userid(user_id)
    _create_temp_database(db, boto3_session=boto3_session)

    delete_temp_table(table, boto3_session=boto3_session)

    # Include timestamp in path to avoid permissions problems with
    # previous sessions
    ts = str(time.time()).replace(".", "")
    path = s3_path_join(table_dir, ts, table)
    dataframe_to_table(df, db, table, path, boto3_session=boto3_session)


@init_athena_params(allow_boto3_session=True)
def dataframe_to_table(
    df: pd.DataFrame,
    database: str,
    table: str,
    location: str,
    mode: str = "overwrite",
    partition_cols: Optional[List[str]] = None,
    boto3_session=None,
    **kwargs,
) -> None:
    """
    Creates a table from a dataframe.

    Args:
        df (pandas.DataFrame): A pandas DataFrame
        database (str): Database name
        table (str): Table name
        location (str): S3 path to where the table should be stored
        mode (str): "overwrite" (default), "append", or "overwrite_partitions"
        partition_cols (List[str]): partition columns (optional)
        boto3_session: optional boto3 session
        **kwargs: arguments for to_parquet
    """

    # Write table
    wr.s3.to_parquet(
        df,
        path=s3_path_join(location, table + ".parquet"),
        dataset=True,
        database=database,
        table=table,
        boto3_session=boto3_session,
        mode=mode,
        partition_cols=partition_cols,
        compression="snappy",
        **kwargs,
    )


def create_database(database: str, **kwargs) -> bool:
    """
    Creates a new database.

    Args:
        database (str): The name of the database

    Returns:
        False if the database already exists, True if
        it has been created.
    """

    if database in (db["Name"] for db in wr.catalog.get_databases()):
        return False
    wr.catalog.create_database(database, **kwargs)
    return True


@init_athena_params(allow_boto3_session=True)
def file_to_table(
    path: str,
    database: str,
    table: str,
    location: str,
    mode: str = "overwrite",
    partition_cols: Optional[List[str]] = None,
    boto3_session=None,
    chunksize=None,
    metadata=None,
    **kwargs,
) -> None:
    """
    Writes a csv, json, or parquet file to a database table.

    Args:
        path (str): The location of the file
        database (str): database name
        table (str): table name
        location (str): s3 file path to table
        mode (str): "overwrite" (default), "append", "overwrite_partitions"
        partition_cols (List[str]): partition columns (optional)
        boto3_session: optional boto3 session
        chunksize Union[int,str]: size of chunks in memory or rows,
            e.g. "100MB", 100000
        metadata: mojap_metadata instance
        **kwargs: arguments for arrow_pd_parser.reader.read
            e.g. use chunksize for very large files, metadata
            to apply metadata
    """

    dfs = reader.read(path, chunksize=chunksize, metadata=metadata, **kwargs)
    if isinstance(dfs, pd.DataFrame):
        # Convert single dataframe to iterator
        dfs = iter([dfs])
    elif mode == "overwrite_partitions":
        raise ValueError(
            "overwrite_partitions and a set chunksize "
            + "can't be used at the same time"
        )

    for df in dfs:
        dataframe_to_table(
            df,
            database,
            table,
            location,
            partition_cols=partition_cols,
            mode=mode,
            boto3_session=boto3_session,
        )
        mode = "append"
