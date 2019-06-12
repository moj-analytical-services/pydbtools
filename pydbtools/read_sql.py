import pandas as pd

from pydbtools.get_athena_query_response import get_athena_query_response

from pydbtools.utils import _athena_meta_conversions, _pd_dtype_dict_from_metadata

from gluejobutils.s3 import delete_s3_object


def read_sql(sql_query, timeout=None, convert_dates=True, cols_as_str=False, *args, **kwargs):

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

    if response["s3"].endswith('.txt'):
        df = pd.read_csv(
            response["s3_path"],
            dtype=object,
            header=None,
            names=['output'],
            *args,
            **kwargs
    )
    else:
        df = pd.read_csv(
            response["s3_path"],
            dtype=dtype,
            parse_dates=parse_dates,
            *args,
            **kwargs
        )

    # Delete both the SQL query and the meta data
    delete_s3_object(response["s3_path"])
    delete_s3_object(response["s3_path"] + ".metadata")

    return df
