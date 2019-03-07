# pydbtools

This is a simple package that let's you query databases using Amazon Athena and get the s3 path to the athena out (as a csv). This is significantly faster than using the the database drivers so might be a good option when pulling in large data.

To install
```
pip
```

package requirements are:

* `pandas` _(preinstalled)_
* `boto3` _(preinstalled)_
* `numpy` _(preinstalled)_
* `s3fs`
* `gluejobutils`

Example:

```python
import pydbtools as pydb
import pandas as pd

response = pydb.get_athena_query_response("SELECT * from database.table limit 10000")

# print out path to athena query output (as a csv)
print(response['s3_path'])

# print out meta data
print(response['meta_data'])

# Read in data using pandas (read everything as a string)
df = pd.read_csv(response['s3_path'].replace('s3://', 's3a://'), dtype = object)
df.head()

```

#### Meta data

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
response <- pydb.get_athena_query_response("SELECT * from database.table limit 10000", return_athena_types=True)

print(response['meta'])
```

If you wish to read your SQL query directly into a pandas dataframe you can use the read_sql function. You can apply `*args` or `**kwargs` into this function which are passed down to `pd.read_csv()`.

```python
import pydbtools as pydb

df = pydb.read_sql("SELECT * FROM database.table limit 1000")
df.head()
```

#### Notes:

- Amazon Athena using a flavour of SQL called presto docs can be found [here](https://prestodb.io/docs/current/)
- To query a date column in Athena you need to specify that your value is a date e.g. `SELECT * FROM db.table WHERE date_col > date '2018-12-31'`
- To query a datetime or timestamp column in Athena you need to specify that your value is a timestamp e.g. `SELECT * FROM db.table WHERE datetime_col > timestamp '2018-12-31 23:59:59'`
- Note dates and datetimes formatting used above. See more specifics around date and datetimes [here](https://prestodb.io/docs/current/functions/datetime.html)
- To specify a string in the sql query always use '' not "". Using ""'s means that you are referencing a database, table or col, etc.
- When data is pulled back into rStudio the column types are either R characters (for any col that was a dates, datetimes, characters) or doubles (for everything else).


See changelog for release changes
