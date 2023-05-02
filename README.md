# pydbtools

A package that is used to run SQL queries speficially configured for the Analytical Platform. This packages uses AWS Wrangler's Athena module but adds additional functionality (like Jinja templating, creating temporary tables) and alters some configuration to our specification. 

## Installation

> Requires a pip release above 20.

```bash
## To install from pypi
pip install pydbtools

## Or install from git with a specific release
pip install "pydbtools @ git+https://github.com/moj-analytical-services/pydbtools@v4.0.1"
```

## Quickstart guide

The [examples directory](examples) contains more detailed notebooks demonstrating the use of this library, many of which are borrowed from the [mojap-aws-tools-demo repo](https://github.com/moj-analytical-services/mojap-aws-tools-demo). 

### Read an SQL Athena query into a pandas dataframe

```python
import pydbtools as pydb
df = pydb.read_sql_query("SELECT * from a_database.table LIMIT 10")
```

### Run a query in Athena

```python
response = pydb.start_query_execution_and_wait("CREATE DATABASE IF NOT EXISTS my_test_database")
```

### Create a temporary table to do further separate SQL queries on later

```python
pydb.create_temp_table("SELECT a_col, count(*) as n FROM a_database.table GROUP BY a_col", table_name="temp_table_1")
df = pydb.read_sql_query("SELECT * FROM __temp__.temp_table_1 WHERE n < 10")

pydb.dataframe_to_temp_table(my_dataframe, "my_table")
df = pydb.read_sql_query("select * from __temp__.my_table where year = 2022")
```

## Introduction

This package is a wrapper for [awswrangler](https://aws-data-wrangler.readthedocs.io/en/2.3.0/what.html) that which presets/defines some of the input parameters to the athena module functions to align with our platform setup. See the [awswrangler API reference documentation for Athena](https://aws-data-wrangler.readthedocs.io/en/2.3.0/api.html#amazon-athena) to see what functions you can call from pydbtools.

The function parameters that are locked down / altered by `pydbtools` are:
- **boto3_session:** This is auto generated by `pydbtools` (in order to grab the user credentials from the sts client - this is needed for the R version of this package which calls this package under the hood. In short forcing refreshed credentials are needed in R as boto3 credentials timeout and do not refresh when using reticulate, though this does not apply to the latest version of the platform currently being rolled out.)
- **s3_output:** The S3 path where database queries are written to. This is defined by `pydbtools` based on the IAM user/role calling the query (ensures that each role can only read/write to a S3 path only they can access).
- **database:** Will either be set to `None` or `__temp__` depending on other user parameters (if `ctas_approach=True`). `__temp__` is an alias to an autogenerated temp database name which is generated from `pydbtools` again based on the IAM user/role. References to this temporary database can be referenced by the keyword `__temp__` in SQL queries see additional functionality to awswrangler section.
- **sql:** We allows reference to the database name `__temp__` which is an alias to a user specific temporary database. When a function call has an SQL parameter the SQL is checked with an SQL parser and then any reference to `__temp__` as a database is replaced with the actual database name which is autogenerated. This replacement only occurs for `SELECT` queries.
- **pyarrow_additional_kwargs:** This is set to `{"coerce_int96_timestamp_unit": "ms", "timestamp_as_object": True}` by default. Doing this solves [this awswrangler issue](https://github.com/awslabs/aws-data-wrangler/issues/592))

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

See [the example notebook](examples/create_temporary_tables.ipynb) for a more detailed example.

### Create databases and tables

```python
import pydbtools as pydb
import pandas as pd

pydb.create_database("my_db")
pydb.file_to_table(
    "local_file_path/data.csv", 
    database="my_db",
    table="my_table",
    location="s3://my_s3_location/my_table"
)
pydb.dataframe_to_table(
    my_dataframe, 
    database="my_db",
    table="my_other_table",
    location="s3://my_s3_location/my_other_table"
)
pydb.create_table(
    "select * from my_db.my_other_table where month = 'March'",
    database="my_db",
    table="my_march_table",
    location="s3://my_s3_location/my_other_table"
)
```

See [the notebook on MoJAP tools](examples/mojap_tools_demo.ipynb) for more details.


### Run SQL from a string of statements or a file

It wil often be more convenient to write your SQL in an editor with language support rather than as a Python string. You can create temporary tables within SQL using the syntax below.

```python
import pydbtools as pydb

sql = """
create temp table A as (
    select * from database.table1
    where year = 2021
);

create temp table B as (
    select * from database.table2
    where amount > 10
);

select * from __temp__.A
left join __temp__.B
on A.id = B.id;
"""

with open("queries.sql", "w") as f:
    f.write(sql)
    
with open("queries.sql", "r") as f:
    df = pydb.read_sql_queries(f.read())
```

Multiple `SELECT` queries can be returned as a generator of dataframes using `read_sql_queries_gen`.

See [the notebook on creating temporary tables with SQL](examples/create_temporary_tables_from_sql_file.ipynb) and [the notebook on database administration with SQL](examples/creating_and_maintaining_database_tables_in_athena_from_sql.ipynb) for more detailed examples.

Additionally you can use [Jinja](https://jinja.palletsprojects.com/en/3.0.x/) templating to inject arguments into your SQL.

```python
sql_template = """
SELECT *
FROM {{ db_name }}.{{ table }}
"""
sql = pydb.render_sql_template(sql_template, {"db_name": db_name, "table": "department"})
pydb.read_sql_query(sql)

with open("tempfile.sql", "w") as f:
    f.write("SELECT * FROM {{ db_name }}.{{ table_name }}")
sql = pydb.get_sql_from_file("tempfile.sql", jinja_args={"db_name": db_name, "table_name": "department"})
pydb.read_sql_query(sql)
"""
```

See the [notebook on SQL templating](examples/sql_templating.ipynb) for more details.
 
### Delete databases, tables and partitions together with the data on S3

```python
import pydbtools as pydb

pydb.delete_partitions_and_data(database='my_database', table='my_table', expression='year = 2020 or year = 2021')
pydb.delete_table_and_data(database='my_database', table='my_table')
pydb.delete_database('my_database')

# These can be used for temporary databases and tables.
pydb.delete_table_and_data(database='__temp__', table='my_temp_table')
```

For more details see [the notebook on deletions](examples/delete_databases_tables_and_partitions.ipynb).

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

### Setting the region

In order to run queries, Athena needs to output its results into a staging bucket in S3. The aws region passed to awswrangler needs to be the same as the region of that bucket. This is usually the same as that set by the `AWS_DEFAULT_REGION` set within your underlying environment. However, in cases of cross-region working, you can specify the region for Athena to access by setting `AWS_ATHENA_QUERY_REGION` as an environment variable.

# DEPRECATED

## Functions

The functions:
- `pydbtools.get_athena_query_response`
- `pydbtools.read_sql`

Are now deprecated and calls to these functions will raise an warning. They have been replaced by `pydbtools.start_query_execution_and_wait` and `pydbtools.read_sql_query`.


#### Notes:

- Amazon Athena using a flavour of SQL called presto docs can be found [here](https://prestodb.io/docs/current/)
- To query a date column in Athena you need to specify that your value is a date e.g. `SELECT * FROM db.table WHERE date_col > date '2018-12-31'`
- To query a datetime or timestamp column in Athena you need to specify that your value is a timestamp e.g. `SELECT * FROM db.table WHERE datetime_col > timestamp '2018-12-31 23:59:59'`
- Note dates and datetimes formatting used above. See more specifics around date and datetimes [here](https://prestodb.io/docs/current/functions/datetime.html)
- To specify a string in the sql query always use '' not "". Using ""'s means that you are referencing a database, table or col, etc.

See changelog for release changes.
