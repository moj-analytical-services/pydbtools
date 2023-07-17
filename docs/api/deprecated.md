# Deprecated

The functions:

- `pydbtools.get_athena_query_response`

- `pydbtools.read_sql`

Are now deprecated and calls to these functions will raise an warning. They have been replaced by `pydbtools.start_query_execution_and_wait` and `pydbtools.read_sql_query`.
