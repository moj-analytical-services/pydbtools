import numpy as np

from gluejobutils.s3 import s3_path_to_bucket_key, check_for_s3_file
import os
from s3fs import S3FileSystem

# pydbtools will create a new a new S3 object (then delete it post read). In the first call read
# the cache is empty but then filled. If pydbtools is called again the cache is referenced and
# you get an NoFileError.
# Setting cachable to false fixes this. cachable is class object from fsspec.AbstractFileSystem
# which S3FileSystem inherits.
S3FileSystem.cachable = False


def get_file(s3_path, check_exists=True):
    """
    Returns an file using s3fs without caching objects (workaround for issue #10).

    s3_path: path to file in S3 e.g. s3://bucket/object/path.csv
    check_exists: If True (default) will check for s3 file existance before returning file.
    """
    b, k = s3_path_to_bucket_key(s3_path)
    if check_exists:
        if not check_for_s3_file(s3_path):
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
}

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
        coltype = _athena_meta_conversions[c["type"]]["pandas"]
        dtype[colname] = np.typeDict[coltype]
        if c["type"] in ["date", "timestamp"]:
            parse_dates.append(colname)

    return (dtype, parse_dates)
