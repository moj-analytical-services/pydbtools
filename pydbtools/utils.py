import inspect
import os
import re
from functools import reduce
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import awswrangler as wr
import boto3
import sql_metadata
import sqlparse
from botocore.credentials import InstanceMetadataFetcher, InstanceMetadataProvider

# Set pydbtool params - if you were so inclined to change them
temp_database_name_prefix = "mojap_de_temp_"
aws_default_region = os.getenv(
    "AWS_ATHENA_QUERY_REGION",
    os.getenv(
        "AWS_DEFAULT_REGION",
        os.getenv("AWS_REGION", "eu-west-1"),
    ),
)

if aws_default_region == "eu-west-1":
    bucket = os.getenv("ATHENA_QUERY_DUMP_BUCKET", "mojap-athena-query-dump")
else:
    try:
        bucket = os.environ["ATHENA_QUERY_DUMP_BUCKET"]
    except KeyError:
        raise KeyError(
            f"""The AWS region is set to {aws_default_region}
            but environment variable ATHENA_QUERY_DUMP_BUCKET was not set.
            Either set AWS_ATHENA_QUERY_REGION to eu-west-1
            or specify the query dump bucket"""
        )

aws_role_regex_rules = [
    (
        r"@[a-z.-]+.gov.uk$",  # gov email
        r"@[a-z.-]+.gov.uk",
    ),
    (
        r"alpha_user_",  # alpha user
        None,
    ),
    (
        r"alpha_app_",  # alpha app
        None,
    ),
    (
        r"^[a-z0-9]{7,8}-airflow_",  # data engineering airflow
        r"[a-z0-9]{7,8}-",
    ),
    (
        r"airflow_",  # data engineering airflow
        None,
    ),
    (
        r"^[0-9]+$",  # numeric
        None,
    ),
    (
        r"^githubactions$",  # GitHub action for e2e
        None,
    ),
    (
        r"^[a-z0-9]{7,8}-airflow-",  # Analytical Platform Airflow
        r"^[^-]+-"
    ),
]


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


def get_user_id_and_table_dir(
    boto3_session=None, force_ec2: bool = False, region_name: str = None
) -> Tuple[str, str]:
    region_name = _set_region_name(region_name)

    if boto3_session is None:
        boto3_session = get_boto_session(force_ec2=force_ec2, region_name=region_name)

    sts_client = boto3_session.client("sts")
    sts_resp = sts_client.get_caller_identity()
    out_path = s3_path_join("s3://" + bucket, sts_resp["UserId"])
    if out_path[-1] != "/":
        out_path += "/"

    return (sts_resp["UserId"], out_path)


def get_database_name_from_userid(user_id: str) -> str:
    """
    Obtain unique database name for temporary database
    from various forms of user id
    """
    # Remove chunk before last ":"
    unique_db_name = user_id.split(":")[-1].lower()

    # Loop through valid role rules
    valid_user = False
    for role_rule, role_sub_rule in aws_role_regex_rules:
        # Apply substitution rule if provided and a match
        if re.search(role_rule, unique_db_name) is not None:
            if role_sub_rule is not None:
                unique_db_name = re.sub(role_sub_rule, "", unique_db_name)

            # Set valid user to true if matches a rule
            valid_user = True

            # Break loop once we've found first matching rule
            break

    # Raise error if user doesn't match one of the set rules
    if not valid_user:
        raise ValueError(f"Invalid user: {user_id}")

    # Replace - with _
    unique_db_name = unique_db_name.replace("-", "_")

    # Only use permitted characters for AWS Athena databases
    unique_db_name = "".join(c for c in unique_db_name if c.isalnum() or c == "_")

    unique_db_name = temp_database_name_prefix + unique_db_name
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
