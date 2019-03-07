# pydbtools

This is a simple package that let's you query databases using Amazon Athena and get the s3 path to the athena out (as a csv). This is significantly faster than using the the database drivers so might be a good option when pulling in large data.

To install
```
pip
```

package requirements are:

* `pandas` _(preinstalled)_
* `boto3` _(preinstalled)_

Example:
```python
import pydbtools as pydb

response = pydb.get_athena_query_response("SELECT * from crest_v1.flatfile limit 10000", out_path = "s3://my-bucket/__temp__")

# print out path to athena query output (as a csv)
print(response['s3_path'])

# print out meta data
print(response['meta_data'])

# Read in data using whatever csv reader you want
s3_path_stripped = gsub("s3://", "", response$s3_path)
df <- s3tools::read_using(FUN = readr::read_csv, s3_path=s3_path_stripped)

```

#### Meta data

The output from dbtools::get_athena_query_response(...) is a list one of it's keys is `meta`. The meta key is a list where each element in this list is the name (`name`) and data type (`type`) for each column in your athena query output. For example for this table output:

|col1|col2|
|---|---|
|1|2018-01-01|
|2|2018-01-02|
...

Would have a meta like:

```
response$meta[[1]]$name # col1
response$meta[[1]]$type # int

response$meta[[1]]$name # col2
response$meta[[1]]$type # date

```

The meta types follow those listed as the generic meta data types used in [etl_manager](https://github.com/moj-analytical-services/etl_manager). If you want the actual athena meta data instead you can get them instead of the generic meta data types by setting the `return_athena_types` input parameter to `TRUE` e.g.

```
response <- dbtools::get_athena_query_response("SELECT * from crest_v1.flatfile limit 10000", return_athena_types=TRUE)

print(response$meta)
```

#### Notes:

- Amazon Athena using a flavour of SQL called presto docs can be found [here](https://prestodb.io/docs/current/)
- To query a date column in Athena you need to specify that your value is a date e.g. `SELECT * FROM db.table WHERE date_col > date '2018-12-31'`
- To query a datetime or timestamp column in Athena you need to specify that your value is a timestamp e.g. `SELECT * FROM db.table WHERE datetime_col > timestamp '2018-12-31 23:59:59'`
- Note dates and datetimes formatting used above. See more specifics around date and datetimes [here](https://prestodb.io/docs/current/functions/datetime.html)
- To specify a string in the sql query always use '' not "". Using ""'s means that you are referencing a database, table or col, etc.
- When data is pulled back into rStudio the column types are either R characters (for any col that was a dates, datetimes, characters) or doubles (for everything else).


#### Changelog:

## v0.0.2 - 2018-10-12

- `timeout` is now an input parameter to `get_athena_query_response` if not set there is no timeout for the athena query.
- `get_athena_query_response` will now print out the athena_client response if the athena query fails.

## v0.0.1 - First Release
