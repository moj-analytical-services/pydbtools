from jinja2 import Template


def get_sql_from_file(filepath: str, jinja_args: dict = None, **kwargs) -> str:
    """
    Read in an SQL file and inject arguments with Jinja (if given params).
    Returns the SQL as a str.

    Args:
        filepath (str): A filepath to your SQL file.
        jinja_args (dict, optional): If not None, will pass the read
            in SQL file through a jinja template to render the template.
            Otherwise will just return the SQL file as is. Defaults to None.
        kwargs: passed to the open() call.
    """
    with open(filepath, **kwargs) as f:
        sql = "".join(f.readlines())
    if jinja_args:
        sql = render_sql_template(sql, jinja_args)
    return sql


def render_sql_template(sql: str, jinja_args: dict) -> str:
    """
    Takes a SQL file templated with Jinja and then injects arguments.
    Returns the injected SQL.

    Args:
        sql_file (str): Path to SQL file
        args (dict): Arguments that is referenced in the SQL file

    Returns:
        str: SQL string that has args rendered into it
    """
    return Template(sql).render(**jinja_args)
