import boto3
import time
import os
import warnings

from pydbtools.utils import (
    _athena_meta_conversions,
    get_user_id_and_table_dir,
    get_boto_client,
    get_database_name_from_userid,
    replace_temp_database_name_reference,
)


def get_athena_query_response(
    sql_query: str,
    return_athena_types: bool = False,
    timeout: int = None,
    force_ec2: bool = False,
    region_name: str = "eu-west-1",
):
    """
    [DEPRECATED] See start_query_execution_and_wait.

    Runs an SQL query against our Athena database and returns a tuple.
    The first argument is an S3 path to the resulting output and the second
    is a dictionary of meta data for the table.

    sql_query: String with the SQL query you want to run against our athena
    databases.

    return_athena_types: Boolean specifying if the column definitions in the
    meta data should be named after the athena types (True) or as our agnostic
    meta data types used in etl_manager (False). Default is False.

    timeout: . Will raise warining if not None.
    Integer specifying the number of seconds to wait before giving up
    on the Athena query. If set to None (default) the query will wait
    indefinitely. .

    force_ec2: Boolean specifying if the user wants to force boto to get the
    credentials from the EC2. This is for dbtools which is the R wrapper that
    calls this package via reticulate and requires credentials to be refreshed
    via the EC2 instance (and therefore sets this to True) - this is not
    necessary when using this in Python. Default is False.
    """
    usage_warning = (
        "This function is deprecated and will be removed "
        "in future releases. Please use the "
        "start_query_execution_and_wait function instead."
    )

    warnings.warn(usage_warning)
    user_id, out_path = get_user_id_and_table_dir(
        boto3_session=None, force_ec2=force_ec2, region_name=region_name
    )

    temp_db_name = get_database_name_from_userid(user_id)
    sql_query = replace_temp_database_name_reference(sql_query, temp_db_name)

    out_path = os.path.join(out_path, "__athena_query_dump__/")
    athena_client = get_boto_client(
        client_name="athena", force_ec2=force_ec2, region_name=region_name
    )

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
