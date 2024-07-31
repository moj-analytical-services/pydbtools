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

The [examples directory](docs/examples) contains more detailed notebooks demonstrating the use of this library, many of which are borrowed from the [mojap-aws-tools-demo repo](https://github.com/moj-analytical-services/mojap-aws-tools-demo).

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

## Notes

- Amazon Athena using a flavour of SQL called trino. Docs can be found [here](https://trino.io/docs/current/language.html)
- To query a date column in Athena you need to specify that your value is a date e.g. `SELECT * FROM db.table WHERE date_col > date '2018-12-31'`
- To query a datetime or timestamp column in Athena you need to specify that your value is a timestamp e.g. `SELECT * FROM db.table WHERE datetime_col > timestamp '2018-12-31 23:59:59'`
- Note dates and datetimes formatting used above. See more specifics around date and datetimes [here](https://prestodb.io/docs/current/functions/datetime.html)
- To specify a string in the sql query always use '' not "". Using ""'s means that you are referencing a database, table or col, etc.
- If you are working in an environment where you cannot change the default AWS region environment
variables you can set `AWS_ATHENA_QUERY_REGION` which will override these.
- You can override the bucket where query results are outputted to with the `ATHENA_QUERY_DUMP_BUCKET` environment variable.
This is mandatory if you set the region to something other than `eu-west-1`.

See changelog for release changes.
