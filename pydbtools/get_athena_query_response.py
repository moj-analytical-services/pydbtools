import boto3
import pandas as pd
import io
import time
import os

from pydbtools.utils import _athena_meta_conversions

def get_athena_query_response(sql_query, return_athena_types=False, timeout=None):

    # Get role specific path for athena output
    bucket = "alpha-athena-query-dump"

    sts_client = boto3.client('sts')
    sts_resp = sts_client.get_caller_identity()

    out_path = os.path.join('s3://', bucket, sts_resp['UserId'], "__athena_temp__/")

    if out_path[-1] != '/':
      out_path += '/'

    # Run the athena query
    athena_client = boto3.client('athena', 'eu-west-1')
    response = athena_client.start_query_execution(
        QueryString=sql_query,
        ResultConfiguration={
            'OutputLocation': out_path,
      }
    )

    sleep_time = 2
    counter = 0
    while True:
        athena_status = athena_client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])
        if athena_status['QueryExecution']['Status']['State'] == "SUCCEEDED":
            break
        elif athena_status['QueryExecution']['Status']['State'] in ['QUEUED','RUNNING']:
            # print('waiting...')
            time.sleep(sleep_time)
        elif athena_status['QueryExecution']['Status']['State'] == 'FAILED':
            raise ValueError("athena failed - response error:\n {}".format(athena_status['QueryExecution']['Status']['StateChangeReason']))
        else:
            raise ValueError("athena failed - unknown reason (printing full response):\n {athena_status}".format(athena_status))

        counter += 1
        if timeout:
          if counter*sleep_time > timeout :
              raise ValueError('athena timed out')

    result_response = athena_client.get_query_results(QueryExecutionId=athena_status['QueryExecution']['QueryExecutionId'], MaxResults=1)
    s3_path = athena_status['QueryExecution']['ResultConfiguration']['OutputLocation']
    if return_athena_types:
        meta = [{'name':c['Name'], 'type' : c['Type']} for c in result_response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    else:
        meta = [{'name':c['Name'], 'type' : _athena_meta_conversions[c['Type']]['etl_manager']} for c in result_response['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    return {'s3_path' : s3_path, 'meta' : meta}