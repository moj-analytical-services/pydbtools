from typing import Tuple
import os
import re
import sqlparse
import inspect
import boto3
from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher

# Set pydbtool params - if you were so inclined to change them
bucket = "mojap-athena-query-dump"
temp_database_name_prefix = "mojap_de_temp_"
aws_default_region = os.getenv(
    "AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "eu-west-1")
)


def _set_region_name(region_name: str):
    if region_name is None:
        return aws_default_region
    else:
        return region_name


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
            "When querying a temporary database, "
            "__temp__ should not be wrapped in quotes"
        )


def clean_query(sql: str, fmt_opts: dict = None) -> str:
    """
    removes trailing whitespace, newlines and final
    semicolon from sql for use with
    sqlparse package

    Args:
        sql (str): The raw SQL query
        fmt_opts (dict): Dictionary of params to pass to sqlparse.format.
        If None then sqlparse.format is not called.
    Returns:
        str: The cleaned SQL query
    """
    sql = " ".join(sql.splitlines()).strip().rstrip(";")
    if fmt_opts:
        sql = sqlparse.format(sql, **fmt_opts)
    return sql


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
    boto3_session=None, force_ec2: bool = False, region_name: str = None
) -> Tuple[str, str]:

    region_name = _set_region_name(region_name)

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
    force_ec2: bool = False, region_name: str = None,
):
    region_name = _set_region_name(region_name)

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
    region_name: str = None,
):

    region_name = _set_region_name(region_name)

    if boto3_session is None:
        boto3_session = get_boto_session(force_ec2=force_ec2, region_name=region_name)

    return boto3_session.client(client_name)
