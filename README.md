# pydbtools

## Installation

> Requires a pip release above 20.

```bash
## To install from pypi
pip install pydbtools

## Or install from git with a specific release
pip install "pydbtools @ git+https://github.com/moj-analytical-services/pydbtools@v3.0.0"
```

## Quickstart guide

### Read an SQL Athena query into a pandas dataframe

```python
import pydbtools as pydb
df = pydb.read_sql("SELECT * from a_database.table LIMIT 10")
```

### Run a query in Athena

```python
response = pydb.start_query_execution_and_wait("CREATE DATABASE IF NOT EXISTS my_test_database")
```

### Create a temporary table to do further separate SQL queries on later

```python
pydb.create_temp_table("SELECT a_col, count(*) as n FROM a_database.table GROUP BY a_col", table_name="temp_table_1")
df = pydb.read_sql_query("SELECT * FROM __temp__.temp_table_1 WHERE n < 10")
```

## Introduction

This package is a wrapper for [awswrangler](https://aws-data-wrangler.readthedocs.io/en/2.3.0/what.html) that which presets/defines some of the input parameters to the athena module functions to align with our platform setup. See the [awswrangler API reference documentation for Athena](https://aws-data-wrangler.readthedocs.io/en/2.3.0/api.html#amazon-athena) to see what functions you can call from pydbtools.

The function parameters that are locked down / altered by `pydbtools` are:
- **boto3_session:** This is auto generated by `pydbtools` (in order to grab the user credentials from the sts client - this is needed for the R version of this package which calls this package under the hood. In short forcing refreshed credentials are needed in R as boto3 credentials timeout and do not refresh when using reticulate (at least currently))
- **s3_output:** The S3 path where database queries are written to. This is defined by `pydbtools` based on the IAM user/role calling the query (ensures that each role can only read/write to a S3 path only they can access).
- **database:** Will either be set to `None` or `__temp__` depending on other user parameters (if `ctas_approach=True`). `__temp__` is an alias to an autogenerated temp database name which is generated from `pydbtools` again based on the IAM user/role. References to this temporary database can be referenced by the keyword `__temp__` in SQL queries see additional functionality to awswrangler section.
- **sql:** We allows reference to the database name `__temp__` which is an alias to a user specific temporary database. When a function call has an SQL parameter the SQL is checked with an SQL parser and then any reference to `__temp__` as a database is replaced with the actual database name which is autogenerated. This replacement only occurs for `SELECT` queries.

## Additional Functionality

As well as acting as a wrapper function for awswrangler this package also allows you to do the following:

### Run query and wait for a response

This function essentially calls two functions from `awswrangler.athena`. First `start_query_execution` followed by `wait_query`.

```python
import pydbtools as pydb

response = pydb.start_query_execution_and_wait("SELECT * from a_database.table LIMIT 10")
```

### Create Temporary Tables

You can use the `create_temp_table` function to write SQL to create a store a temporary table that sits in your `__temp__` database.

```python
import pydbtools as pydb

pydb.create_temp_table("SELECT * from a_database.table LIMIT 10", table_name="temp_table_1")
df = pydb.read_sql_query("SELECT * from __temp__.temp_table_1")
df.head()
```

## Usage / Examples

### Simple 

```python
import pydbtools as pydb

# Run a query using pydbtools
response = pydb.start_query_execution_and_wait("CREATE DATABASE IF NOT EXISTS my_test_database")

# Read data from an athena query directly into pandas
pydb.read_sql("SELECT * from a_database.table LIMIT 10")

# Create a temp table to do further seperate SQL queries later on
pydb.create_temp_table("SELECT a_col, count(*) as n FROM a_database.table GROUP BY a_col", table_name="temp_table_1")
df = pydb.read_sql_query("SELECT * FROM __temp__.temp_table_1 WHERE n < 10")
```

### More advanced usage

Get the actual name for your temp database, create your temp db then delete it using awswrangler (note: `awswrangler` will raise an error if the database does not exist)

```python
import awswrangler as wr
import pydbtools as pydb

user_id, out_path = pydb.get_user_id_and_table_dir()
temp_db_name = pydb.get_database_name_from_userid(user_id)
print(temp_db_name)
pydb.create_temp_table()
print(wr.catalog.delete_database(name=temp_db_name))
```

# DEPRECATED

## Functions

The functions:
- `pydbtools.get_athena_query_response`
- `pydbtools.read_sql`

Are now deprecated and calls to these functions will raise an warning. They have been replaced by `pydbtools.start_query_execution_and_wait` and `pydbtools.read_sql_query`.

## Docs for versions below v3.0.0

This is a simple package that let's you query databases using Amazon Athena and get the s3 path to the athena out (as a csv). This is significantly faster than using the the database drivers so might be a good option when pulling in large data. By default, data is converted into a pandas dataframe with equivalent column data types as the Athena table - see "Meta Data" section below.

Note to use this package you need to be added to the StandardDatabaseAccess IAM Policy on the Analytical Platform. Please contact the team if you require access.

To install...

```
pip install pydbtools
```

Or from github...

```
pip install git+git://github.com/moj-analytical-services/pydbtools.git#egg=pydbtools
```

package requirements are:

* `pandas` _(preinstalled)_
* `boto3` _(preinstalled)_
* `numpy` _(preinstalled)_
* `s3fs`
* `gluejobutils`

## Usage

Most simple way to use pydbtools. This will return a pandas df reprentation of the data (with matching meta data).

```python
import pydbtools as pydb

# Run SQL query and return as a pandas df
df = pydb.read_sql("SELECT * from database.table limit 10000")
df.head()
```

You might want to cast the data yourself or read all the columns as strings.

```python
import pydbtools as pydb

# Run SQL query and return as a pandas df
df = pydb.read_sql("SELECT * from database.table limit 10000", cols_as_str=True)
df.head()

df.dtypes # all objects
```

You can also pass additional arguments to the pandas.read_csv that reads the resulting Athena SQL query.
Note you cannot pass dtype as this is specified within the `read_sql` function.

```python
import pydbtools as pydb

# pass nrows parameter to pandas.read_csv function
pydb.read_sql("SELECT * from database.table limit 10000", nrows=20)
```

If you didn't want to read the data into pandas you can run the SQL query and get the s3 path and meta data 
of the output using the get_athena_query_response. The data is then read in using `boto3`, `io` and `csv`. 


```python
import pydbtools as pydb
import io
import csv
import boto3

response = pydb.get_athena_query_response("SELECT * from database.table limit 10000")

# print out path to athena query output (as a csv)
print(response['s3_path'])

# print out meta data
print(response['meta'])

# Read the csv into a string in memory
s3_resource = boto3.resource('s3')
bucket, key = response['s3_path'].replace("s3://", "").split('/', 1)
obj = s3_resource.Object(bucket, key)
text = obj.get()['Body'].read().decode('utf-8')

# Use csv reader to print the outputting csv
reader = csv.reader(text.split('\n'), delimiter=',')
for row in reader:
    print('\t'.join(row))
```

## Meta data

The output from get_athena_query_response(...) is a dictionary one of it's keys is `meta`. The meta key is a list where each element in this list is the name (`name`) and data type (`type`) for each column in your athena query output. For example for this table output:

|col1|col2|
|---|---|
|1|2018-01-01|
|2|2018-01-02|
...

Would have a meta like:

```python
for m in response['meta']:
    print(m['name'], m['type'])
```

output:

```
> col1 int
> col1 date
```

The meta types follow those listed as the generic meta data types used in [etl_manager](https://github.com/moj-analytical-services/etl_manager). If you want the actual athena meta data instead you can get them instead of the generic meta data types by setting the `return_athena_types` input parameter to `True` e.g.

```python
response = pydb.get_athena_query_response("SELECT * from database.table limit 10000", return_athena_types=True)

print(response['meta'])
```

If you wish to read your SQL query directly into a pandas dataframe you can use the read_sql function. You can apply `*args` or `**kwargs` into this function which are passed down to `pd.read_csv()`.

```python
import pydbtools as pydb

df = pydb.read_sql("SELECT * FROM database.table limit 1000")
df.head()
```

### Meta data conversion

Below is a table that explains what the conversion is from our data types to a pandas df (using the `read_sql` function):

| data type | pandas column type| Comment                                                                                 |
|-----------|-------------------|-----------------------------------------------------------------------------------------|
| character | object            | [see here](https://stackoverflow.com/questions/34881079/pandas-distinction-between-str-and-object-types)|
| int       | np.float64        | Pandas integers do not allow nulls so using floats                                      |
| long      | np.float64        | Pandas integers do not allow nulls so using floats                                      |
| date      | pandas timestamp  |                                                                                         |
| datetime  | pandas timestamp  |                                                                                         |
| boolean   | np.bool           |                                                                                         |
| float     | np.float64        |                                                                                         |
| double    | np.float64        |                                                                                         |
| decimal    | np.float64        |                                                                                         |

## Unit tests
Unit tests run in unittest through Poetry. Run `poetry run python -m unittest` to activate them. If you've changed any dependencies, run `poetry update` first.

The tests run against a test Glue database callled `dbtools`. They use data stored on s3 in `alpha-dbtools-test-bucket`.

#### Notes:

- Amazon Athena using a flavour of SQL called presto docs can be found [here](https://prestodb.io/docs/current/)
- To query a date column in Athena you need to specify that your value is a date e.g. `SELECT * FROM db.table WHERE date_col > date '2018-12-31'`
- To query a datetime or timestamp column in Athena you need to specify that your value is a timestamp e.g. `SELECT * FROM db.table WHERE datetime_col > timestamp '2018-12-31 23:59:59'`
- Note dates and datetimes formatting used above. See more specifics around date and datetimes [here](https://prestodb.io/docs/current/functions/datetime.html)
- To specify a string in the sql query always use '' not "". Using ""'s means that you are referencing a database, table or col, etc.
- When data is pulled back into rStudio the column types are either R characters (for any col that was a dates, datetimes, characters) or doubles (for everything else).

See changelog for release changes
