# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v5.5.12 - 2023-11-14

- use different pypi publish action

## v5.5.11 - 2023-11-14

- Use new pypi publish action

## v5.5.10 - 2023-11-10

- Update dependencies for security
- Updates pypi action for Trusted Publisher


## v5.5.9 - 2023-09-29

- Fix for SSO without breaking AP

## v5.5.8 - 2023-08-01

- Adds python 3.11 to test actions

## v5.5.7 - 2023-07-25

- Expands python requirements to allow 3.11

## v5.5.6 - 2023-05-05

- Reverts back to v5.5.3 due to SSO fixes inadvertently breaking things for AP users

## v5.5.5 - 2023-05-02

- Some further fixes for SSO

## v5.5.4 - 2023-04-26

- Attempt at fixing user ID parsing for SSO roles

## v5.5.3 - 2023-03-06

- Fixed issue in create_temp_table

## v5.5.2 - 2023-02-24

- Fixed issue in dataframe_to_temp_table

## v5.5.1 - 2023-02-13

- Fixed issue in `create_table`

## v5.5.0 - 2023-02-07

## Added

- Function that creates database, see `pydbtools.create_database`
- Functions that create tables from files, SQL or dataframes, see `pydbtools.file_to_table`, `pydbtools.create_table` and `pydbtools.dataframe_to_table`.

## v5.4.0 - 2023-01-24

- Added `tables` functionality to return dataframe of all tables and columns. Wrapped from wrangler.catalog.tables.

## v5.3.2 - 2022-10-07

- Updated dependencies.

## v5.3.1 - 2022-07-22

- Fix issue when creating temporary tables where tables which start with another table's name could have their data deleted.

## v5.3.0 - 2022-07-13

- No longer supporting Python 3.7 due to numpy security issue.

## v5.2.0 - 2022-02-28

## Added

- Function that creates a temporary table from a `pandas.DataFrame`, see `pydbtools.dataframe_to_temp_table`.

## v5.1.0 - 2022-02-11

## Added

- Function that saves the result of SQL query to a parquet file, see `pydbtools.save_query_as_parquet`.

## v5.0.0 - 2022-01-19

## Added

- Functions that read SQL from strings of multiple statements, see `pydbtools.read_sql_queries` and `pydbtools.read_sql_queries_gen`.
- Functions that delete database entities and the S3 data for those entities, see `pydbtools.delete_database_and_data`, `pydbtools.delete_table_and_data` and `pydbtools.delete_partitions_and_data`.

## Changed

- Temporary database names are now compatible with EKS.
- `ctas_approach` now defaults to `True`, the default behaviour for `awswrangler`, using a temporary database.
- Queries automatically use values for `pyarrow_additional_kwargs` that solve [this awswrangler issue](https://github.com/awslabs/aws-data-wrangler/issues/592)

## v4.0.1 - 2021-09-24

## Changed

- Updated project dependencies to align with other packages we heavily use.

## v4.0.0 - 2021-07-16

## Added

- Jinja templating to SQL see `pydbtools.render_sql_template`.
- Read SQL function that takes an SQL file and returns it as a string (with Jinja arguments if provided) see `pydbtools.get_sql_from_file`.

## Changed

- removed DEPRECATED functions: `pydbtools.get_athena_query_response`
`pydbtools.read_sql`
- Made modules not for access private.
- Added ability to change `bucket`, `temp_database_name_prefix` and `aws_default_region` in `pydbtools.utils`.


## v3.1.1 - 2021-06-25

## Changed

- Fixing bug where the `sqlparse` package (used to check if SQL is viable to be wrapped in a CTAS query) couldn't pass some `WITH` SQL queries that had comments after a comma.

## v3.1.0 - 2021-03-10

### Changed
- If left unspecified `ctas_approach` is set to `False` (instead of `True` which is the default behaviour for `awswrangler`). This is to address on going issues we are finding with ctas_approach:

- [this pydbtools issue](https://github.com/moj-analytical-services/pydbtools/issues/41)
- [this awswrangler issue](https://github.com/awslabs/aws-data-wrangler/issues/592)

## v3.0.1 - 2021-02-12

### Changed
- The `wrangler.py` module now has logging.  To observe the arguments and sql queries being run, turn on logging at DEBUG level using e.g.

```
import logging
logging.basicConfig()
logging.getLogger("pydbtools").setLevel(logging.DEBUG)
```
## v3.0.0 - 2021-01-26

### Changed
- `pydbtools` now acts as a wrapper for the athena module in awswrangler
- Previous functions `get_athena_query_response` and `read_sql` are now deprecated (but still currently work with this release. later releases may remove them).
- Allows users to create temporary tables that are stored in a database aliased by the name `__temp__`. SQL queries will replace any reference to `__temp__` with the real database name before the call.


## v2.0.2 - 2020-11-26

### Fixed
- Can now read `decimal` datatypes. Will treat them as floats, even if they're set to 0 decimal places.

## v2.0.1 - 2020-09-21

### Fixed
- Pinned s3fs version to below 0.5.0 to avoid version conflicts with boto3 and botocore, caused by the addition of aiobotocore in 0.5.0.

## v2.0.0 - 2020-07-14

### Changed
- Changed target bucket for temporary files from Athena queries: now mojap-athena-query-dump rather than alpha-athena-query-dump

## v1.0.3 - 2019-09-19

### Changed

- Added a minor workaround to fix what previous patch tried to fix and failed. See [issue](https://github.com/moj-analytical-services/pydbtools/issues/10).

## v1.0.2 - 2019-09-17

### Changed

- Added a minor workaround to try and fix reading from [s3 issues](https://github.com/pandas-dev/pandas/issues/27528)

## v1.0.1 - 2019-06-18

### Changed

- Removed f-strings in code and made pyproject allow for python 3.5

## v1.0.0 - 2019-06-13

### Changed

- Added the option for `read_sql` to read all columns as strings instead of converting them to meta data that matches the original athena table. (For all cols as strings -> `cols_as_str=True`)
- `read_sql` can now handle Athena txt file outputs e.g. (`SHOW COLUMNS FROM db.table`). It will return a pandas df with one col named `output` where each row is a line in the text file.
- Added option to `get_athena_query_response` to get credentials from the ec2 instance (using the param `force_ec2=True`). This is specifically to fix the [creds refresh/expiry issue in dbtools](https://github.com/moj-analytical-services/dbtools/issues/8).
- Published to pypi

### Deleted

- setup.py

### Added

- pyproject.toml

## v0.0.1 - 2019-03-08 - Initial Release
