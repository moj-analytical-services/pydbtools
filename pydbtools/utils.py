# Some notes on the below:
# - int and bigint: pandas doesn't allow nulls in int columns so have to use float
# - date and datetime: pandas doesn't really have a datetime type it expects datetimes use parse_dates
_athena_meta_conversions = {
    "char": {
        "etl_manager": "character", 
        "pandas": "object"
    },
    "varchar": {
        "etl_manager": "character",
        "pandas": "object"
    },
    "integer": {
        "etl_manager": "int",
        "pandas": "float"
    },
    "bigint": {
        "etl_manager": "long",
        "pandas": "float"
    },
    "date": {
        "etl_manager": "date",
        "pandas": "object"
    },
    "timestamp": {
        "etl_manager": "datetime",
        "pandas": "object"
    },
    "boolean": {
        "etl_manager": "boolean",
        "pandas": "bool"
    },
    "float": {
        "etl_manager": "float",
        "pandas": "float"
    },
    "double": {
        "etl_manager": "double",
        "pandas": "float"
    }
}