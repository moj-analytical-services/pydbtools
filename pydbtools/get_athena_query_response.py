import boto3
import time
import os

from pydbtools.utils import _athena_meta_conversions
from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher


def get_athena_query_response(
    sql_query, return_athena_types=False, timeout=None, force_ec2=False
):
    """
    Runs an SQL query against our Athena database and returns a tuple.
    The first argument is an S3 path to the resulting output and the second
    is a dictionary of meta data for the table.

    sql_query: String with the SQL query you want to run against our athena
    databases.

    return_athena_types: Boolean specifying if the column definitions in the
    meta data should be named after the athena types (True) or as our agnostic
    meta data types used in etl_manager (False). Default is False.

    timeout: Integer specifying the number of seconds to wait before giving up
    on the Athena query. If set to None (default) the query will wait
    indefinitely.

    force_ec2: Boolean specifying if the user wants to force boto to get the
    credentials from the EC2. This is for dbtools which is the R wrapper that
    calls this package via reticulate and requires credentials to be refreshed
    via the EC2 instance (and therefore sets this to True) - this is not
    necessary when using this in Python. Default is False.
    """

    # Get role specific path for athena output
    bucket = "mojap-athena-query-dump"
    rn = "eu-west-1"

    if force_ec2:
        provider = InstanceMetadataProvider(
            iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2)
        )
        creds = provider.load().get_frozen_credentials()
        sts_client = boto3.client(
            "sts",
            region_name=rn,
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            aws_session_token=creds.token,
        )
        athena_client = boto3.client(
            "athena",
            region_name=rn,
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            aws_session_token=creds.token,
        )
    else:
        sts_client = boto3.client("sts", region_name=rn)
        athena_client = athena_client = boto3.client("athena", region_name=rn)

    sts_resp = sts_client.get_caller_identity()
    out_path = os.path.join("s3://", bucket, sts_resp["UserId"], "__athena_temp__/")

    if out_path[-1] != "/":
        out_path += "/"

    # Run the athena query
    response = athena_client.start_query_execution(
        QueryString=sql_query, ResultConfiguration={"OutputLocation": out_path}
    )

    sleep_time = 2
    counter = 0
    while True:
        athena_status = athena_client.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        if athena_status["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
            break
        elif athena_status["QueryExecution"]["Status"]["State"] in [
            "QUEUED",
            "RUNNING",
        ]:
            # print('waiting...')
            time.sleep(sleep_time)
        elif athena_status["QueryExecution"]["Status"]["State"] == "FAILED":
            scr = athena_status["QueryExecution"]["Status"]["StateChangeReason"]
            raise ValueError("athena failed - response error:\n {}".format(scr))
        else:
            raise ValueError(
                """
            athena failed - unknown reason (printing full response):
            {}
            """.format(
                    athena_status
                )
            )

        counter += 1
        if timeout:
            if counter * sleep_time > timeout:
                raise ValueError("athena timed out")

    result_response = athena_client.get_query_results(
        QueryExecutionId=athena_status["QueryExecution"]["QueryExecutionId"],
        MaxResults=1,
    )
    s3_path = athena_status["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    if return_athena_types:
        meta = [
            {"name": c["Name"], "type": c["Type"]}
            for c in result_response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        ]
    else:
        meta = [
            {
                "name": c["Name"],
                "type": _athena_meta_conversions[c["Type"]]["etl_manager"],
            }
            for c in result_response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        ]

    return {"s3_path": s3_path, "meta": meta}
