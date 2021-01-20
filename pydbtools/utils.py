from typing import Tuple
import numpy as np
import os
import re
import sqlparse
from s3fs import S3FileSystem
import inspect
import boto3
from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher
import awswrangler as wr

# pydbtools will create a new a new S3 object (then delete it post read). In the first call read
# the cache is empty but then filled. If pydbtools is called again the cache is referenced and
# you get an NoFileError.
# Setting cachable to false fixes this. cachable is class object from fsspec.AbstractFileSystem
# which S3FileSystem inherits.
S3FileSystem.cachable = False

# Get role specific path for athena output
bucket = "mojap-athena-query-dump"

temp_database_name_prefix = "mojap_de_temp_"


def get_default_args(func):
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }


def check_temp_query(sql: str):
    """
    Checks if a query to a temporary table
    has had __temp__ wrapped in quote marks.

    Args:
        sql (str): an SQL query

    Raises:
        ValueError
    """
    if re.findall(r'["|\']__temp__["|\']\.', sql.lower()):
        raise ValueError(
            "When querying a temporary database, __temp__ should not be wrapped in quotes"
        )


def clean_query(sql: str) -> str:
    """
    removes trailing whitespace, newlines and final
    semicolon from sql for use with
    sqlparse package

    Args:
        sql (str): The raw SQL query

    Returns:
        str: The cleaned SQL query
    """
    return " ".join(sql.splitlines()).strip().rstrip(";")


def replace_temp_database_name_reference(sql: str, database_name: str) -> str:
    """
    Replaces references to the user's temp database __temp__
    with the database_name string provided.

    Args:
        sql (str): The raw SQL query as a string
        database_name (str): The database name to replace __temp__

    Returns:
        str: The new SQL query which is sent to Athena
    """
    # check query is valid and clean
    parsed = sqlparse.parse(clean_query(sql))
    new_query = []
    for query in parsed:
        check_temp_query(str(query))
        for word in str(query).strip().split(" "):
            if "__temp__." in word.lower():
                word = word.lower().replace("__temp__.", f"{database_name}.")
            new_query.append(word)
        if ";" not in new_query[-1]:
            last_entry = new_query[-1] + ";"
        else:
            last_entry = new_query[-1]
        del new_query[-1]
        new_query.append(last_entry)
    return " ".join(new_query)


def get_user_id_and_table_dir(
    boto3_session=None, force_ec2: bool = False, region_name: str = "eu-west-1"
) -> Tuple[str, str]:

    if boto3_session is None:
        boto3_session = get_boto_session(force_ec2=force_ec2, region_name=region_name)

    sts_client = boto3_session.client("sts")
    sts_resp = sts_client.get_caller_identity()
    out_path = os.path.join("s3://", bucket, sts_resp["UserId"])
    if out_path[-1] != "/":
        out_path += "/"

    return (sts_resp["UserId"], out_path)


def get_database_name_from_userid(user_id: str) -> str:
    unique_db_name = user_id.split(":")[-1].split("-", 1)[-1].replace("-", "_")
    unique_db_name = temp_database_name_prefix + unique_db_name
    return unique_db_name


def get_boto_session(
    force_ec2: bool = False, region_name: str = "eu-west-1",
):

    kwargs = {"region_name": region_name}
    if force_ec2:
        provider = InstanceMetadataProvider(
            iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2)
        )
        creds = provider.load().get_frozen_credentials()
        kwargs["aws_access_key_id"] = creds.access_key
        kwargs["aws_secret_access_key"] = creds.secret_key
        kwargs["aws_session_token"] = creds.token

    return boto3.Session(**kwargs)


def get_boto_client(
    client_name: str,
    boto3_session=None,
    force_ec2: bool = False,
    region_name: str = "eu-west-1",
):

    if boto3_session is None:
        boto3_session = get_boto_session(force_ec2=force_ec2, region_name=region_name)

    return boto3_session.client(client_name)


def get_file(s3_path: str, check_exists: bool = True):
    """
    Returns an file using s3fs without caching objects (workaround for issue #10).

    s3_path: path to file in S3 e.g. s3://bucket/object/path.csv
    check_exists: If True (default) will check for s3 file existence before returning file.
    """
    b, k = s3_path.replace("s3://", "").split("/", 1)
    if check_exists:
        if not wr.s3.does_object_exist(s3_path):
            raise FileNotFoundError(f"File not found in S3. full path: {s3_path}")
    fs = S3FileSystem()
    f = fs.open(os.path.join(b, k), "rb")

    return f


# Some notes on the below:
# - int and bigint: pandas doesn't allow nulls in int columns so have to use float
# - date and datetime: pandas doesn't really have a datetime type it expects datetimes use parse_dates
# - string is when athena output is just a text file e.g. SHOW COLUMNS FROM db. Setting as a character
_athena_meta_conversions = {
    "char": {"etl_manager": "character", "pandas": "object"},
    "varchar": {"etl_manager": "character", "pandas": "object"},
    "integer": {"etl_manager": "int", "pandas": "float"},
    "bigint": {"etl_manager": "long", "pandas": "float"},
    "date": {"etl_manager": "date", "pandas": "object"},
    "timestamp": {"etl_manager": "datetime", "pandas": "object"},
    "boolean": {"etl_manager": "boolean", "pandas": "bool"},
    "float": {"etl_manager": "float", "pandas": "float"},
    "double": {"etl_manager": "double", "pandas": "float"},
    "string": {"etl_manager": "character", "pandas": "object"},
    "decimal": {"etl_manager": "decimal", "pandas": "float"},
}


# Two functions below stolen and altered from here:
# https://github.com/moj-analytical-services/dataengineeringutils/blob/metadata_conformance/dataengineeringutils/pd_metadata_conformance.py
def _pd_dtype_dict_from_metadata(athena_meta: list):
    """
    Convert the athena table metadata to the dtype dict that needs to be
    passed to the dtype argument of pd.read_csv. Also return list of columns that pandas needs to convert to dates/datetimes
    """
    # see https://stackoverflow.com/questions/34881079/pandas-distinction-between-str-and-object-types

    parse_dates = []
    dtype = {}

    for c in athena_meta:
        colname = c["name"]
        coltype = _athena_meta_conversions[c["type"]]["pandas"]
        dtype[colname] = np.typeDict[coltype]
        if c["type"] in ["date", "timestamp"]:
            parse_dates.append(colname)

    return (dtype, parse_dates)
