import pandas as pd

# setting s3fs cache to false is to try an fix this Access Denied ListObjectsV2 issue see below
# https://github.com/pandas-dev/pandas/issues/27528
import s3fs
from pydbtools.get_athena_query_response import get_athena_query_response
from pydbtools.utils import _pd_dtype_dict_from_metadata, get_file
from gluejobutils.s3 import delete_s3_object


def read_sql(
    sql_query, timeout=None, convert_dates=True, cols_as_str=False, *args, **kwargs
):
    """
    Takes an athena SQL query and returns a pandas dataframe. The Athena query will write the resulting
    output into a CSV or txt file depending on the type of query. In both instances these will be read into
    pandas using pandas.read_csv. You can pass additional arguments into read_csv using *args and **kwargs.

    sql_query: String with the SQL query you want to run against our athena databases.

    timeout: Integer specifying the number of seconds to wait before giving up on the Athena query.
    If set to None (default) the query will wait indefinitely.

    convert_dates: Boolean specifying if date columns should be converted to datetimes. Default is True.
    
    cols_as_str: Boolean specifying if the data returned should treat all columns as strings rather than
    casting the columns to the pandas equivalent data types of the table in Athena. Default is False.
    """
    # Run the SQL query
    response = get_athena_query_response(
        sql_query=sql_query, return_athena_types=True, timeout=timeout
    )

    # Read in the SQL query
    if cols_as_str:
        dtype = object
        parse_dates = False
    else:
        dtype, parse_dates = _pd_dtype_dict_from_metadata(response["meta"])

    if not convert_dates:
        parse_dates = False

    # returns an file using s3fs without caching objects
    f = get_file(response["s3_path"])
    if response["s3_path"].endswith(".txt"):
        df = pd.read_csv(
            f, dtype=object, header=None, names=["output"], *args, **kwargs
        )
    else:
        df = pd.read_csv(f, dtype=dtype, parse_dates=parse_dates, *args, **kwargs)

    # Delete both the SQL query and the meta data
    delete_s3_object(response["s3_path"])
    delete_s3_object(response["s3_path"] + ".metadata")

    return df
