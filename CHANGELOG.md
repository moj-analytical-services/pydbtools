# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

# v1.0.1

## Changes

- Removed f-strings in code and made pyproject allow for python 3.5

# v1.0.0 Major Update

## Changes

- Added the option for `read_sql` to read all columns as strings instead of converting them to meta data that matches the original athena table. (For all cols as strings -> `cols_as_str=True`)
- `read_sql` can now handle Athena txt file outputs e.g. (`SHOW COLUMNS FROM db.table`). It will return a pandas df with one col named `output` where each row is a line in the text file.
- Added option to `get_athena_query_response` to get credentials from the ec2 instance (using the param (`force_ec2=True`). This is specifically to fix the [issue in dbtools](moj-analytical-services/dbtools#8).
- Published to pypi

## Deleted

- setup.py

## Added

 - pyproject.toml

# v0.0.1 Initial Release
