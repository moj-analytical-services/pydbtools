from typing import Tuple, Optional
import os
import re
import sqlparse
from urllib.parse import urlparse, urljoin, urlunparse
import sql_metadata
import inspect
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.credentials import (
    InstanceMetadataProvider,
    InstanceMetadataFetcher,
)
from functools import reduce
import awswrangler as wr
import warnings

# Set pydbtool params - if you were so inclined to change them
bucket = os.getenv("ATHENA_QUERY_DUMP_BUCKET", "mojap-athena-query-dump")
try:
    bucket_region = wr.s3.get_bucket_region(bucket)
except NoCredentialsError:
    bucket_region = "eu-west-1"
temp_database_name_prefix = "mojap_de_temp_"
aws_default_region = os.getenv(
    "AWS_ATHENA_QUERY_REGION",
    os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "eu-west-1")),
)

if aws_default_region != bucket_region:
    warnings.warn(
        f"""
    Your aws region {aws_default_region} is different from the bucket where
    the query results are saved: {bucket_region}. You can change this for this session
    by setting pydb.utils.aws_default_region = "{bucket_region}".
    You should also set the environment variable:
    AWS_ATHENA_QUERY_REGION = "{bucket_region}" to ensure the correct region is set.
    """
    )


def s3_path_join(base: str, *urls: [str]):
    return reduce(_s3_path_join, urls, base)


def _s3_path_join(base: str, url: str, allow_fragments=True) -> str:
    """
    Joins a base S3 path and a URL. Acts the same as urllib.parse.urljoin,
    which doesn't work for S3 paths.

    Args:
        base (str): Base S3 URL
        url (str):
    """
    p = urlparse(base)
    return urlunparse(p._replace(path=urljoin(p.path, url, allow_fragments=True)))


def _set_aws_session_name():
    if not os.getenv("AWS_ROLE_SESSION_NAME"):
        os.environ["AWS_ROLE_SESSION_NAME"] = _get_role_name_from_env()


def _get_role_name_from_env() -> str:
    aws_role_arn = os.getenv("AWS_ROLE_ARN")
    if aws_role_arn is None:
        raise EnvironmentError("AWS_ROLE_ARN was not found in env")
    return aws_role_arn.split("/")[-1]


def _set_region_name(region_name: str):
    if region_name is None:
        os.environ["AWS_DEFAULT_REGION"] = aws_default_region
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


def clean_query(sql: str, fmt_opts: Optional[dict] = None) -> str:
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
    if fmt_opts is None:
        fmt_opts = {}
    fmt_opts["strip_comments"] = True
    sql = sqlparse.format(sql, **fmt_opts)
    sql = " ".join(sql.splitlines()).strip().rstrip(";")
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

    parsed = sqlparse.parse(sql)
    new_query = []
    for query in parsed:
        check_temp_query(str(query))
        # Get all the separated tokens from subtrees
        fq = list(query.flatten())
        # Join them back together replacing __temp__
        # where necessary
        new_query.append(
            "".join(
                re.sub("^__temp__", database_name, str(word), flags=re.IGNORECASE)
                for word in fq
            )
        )
    # Strip output for consistency, different versions of sqlparse
    # treat a trailing newline differently
    return "".join(new_query).strip()


def clean_user_id(user_id: str) -> str:
    username = user_id.split(":")[-1]
    if "@" in username:
        username = username.split("@")[0]
    username = username.replace("-", "_")
    return username


def get_user_id_and_table_dir(
    boto3_session=None, force_ec2: bool = False, region_name: str = None
) -> Tuple[str, str]:

    region_name = _set_region_name(region_name)

    if boto3_session is None:
        boto3_session = get_boto_session(force_ec2=force_ec2, region_name=region_name)

    sts_client = boto3_session.client("sts")
    sts_resp = sts_client.get_caller_identity()
    user_id = clean_user_id(sts_resp["UserId"])
    out_path = s3_path_join("s3://" + bucket, user_id)
    if out_path[-1] != "/":
        out_path += "/"

    return (user_id, out_path)


def get_database_name_from_userid(clean_user_id: str) -> str:
    unique_db_name = temp_database_name_prefix + clean_user_id
    return unique_db_name


def get_database_name_from_sql(sql: str) -> str:
    """
    Obtains database name from SQL query for use
    by awswrangler.

    Args:
        sql (str): The raw SQL query as a string

    Returns:
        str: The database table name
    """

    for table in sql_metadata.Parser(sql).tables:
        # Return the first database seen in the
        # form "database.table"
        xs = table.split(".")
        if len(xs) > 1:
            return xs[0]

    # Return default in case of failure to parse
    return None


def get_boto_session(
    force_ec2: bool = False,
    region_name: str = None,
):
    # Check for new platform authentication
    if os.getenv("AWS_ROLE_ARN") is not None:
        _set_aws_session_name()

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


def get_table_location(database: str, table: str, **kwargs):
    path = wr.catalog.get_table_location(database, table, **kwargs)
    return path if path.endswith("/") else path + "/"
