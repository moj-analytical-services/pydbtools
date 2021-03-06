# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
