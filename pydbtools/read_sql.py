import numpy as np
import pandas as pd

import get_athena_query_response.*, utils._athena_meta_conversions
from gluejobutils.s3 import delete_s3_object


def read_sql(sql_query, timeout=None, *args, **kwargs):

    # Run the SQL query
    response = get_athena_query_response(sql_query=sql_query, return_athena_types=True, timeout=timeout)

    # Read in the SQL query
    dtype, parse_dates = _pd_dtype_dict_from_metadata(response['meta'])
    s3_path = response['s3_path'].replace('s3://', 's3a://')
    df = pd.read_csv(s3_path, dtype = dtype, parse_dates = parse_dates, *args, **kwargs)

    # Delete both the SQL query and the meta data
    delete_s3_object(response['s3_path'])
    delete_s3_object(response['s3_path']+'.metadata')
    
    return df

# Two functions below stolen and altered from here:
# https://github.com/moj-analytical-services/dataengineeringutils/blob/metadata_conformance/dataengineeringutils/pd_metadata_conformance.py
def _pd_dtype_dict_from_metadata(athena_meta):
    """
    Convert the athena table metadata to the dtype dict that needs to be
    passed to the dtype argument of pd.read_csv. Also return list of columns that pandas needs to convert to dates/datetimes
    """
    # see https://stackoverflow.com/questions/34881079/pandas-distinction-between-str-and-object-types
    
    parse_dates = []
    dtype = {}

    for c in athena_meta:
        colname = c["name"]
        coltype = c["type"]
        coltype = _athena_meta_conversions[coltype]['pandas']
        dtype[colname] = np.typeDict[coltype]
        if coltype in ["date", "timestamp"]:
            parse_dates.append(colname)

    return (dtype, parse_dates)