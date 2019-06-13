# pydbtools

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

#### Notes:

- Amazon Athena using a flavour of SQL called presto docs can be found [here](https://prestodb.io/docs/current/)
- To query a date column in Athena you need to specify that your value is a date e.g. `SELECT * FROM db.table WHERE date_col > date '2018-12-31'`
- To query a datetime or timestamp column in Athena you need to specify that your value is a timestamp e.g. `SELECT * FROM db.table WHERE datetime_col > timestamp '2018-12-31 23:59:59'`
- Note dates and datetimes formatting used above. See more specifics around date and datetimes [here](https://prestodb.io/docs/current/functions/datetime.html)
- To specify a string in the sql query always use '' not "". Using ""'s means that you are referencing a database, table or col, etc.
- When data is pulled back into rStudio the column types are either R characters (for any col that was a dates, datetimes, characters) or doubles (for everything else).

See changelog for release changes
