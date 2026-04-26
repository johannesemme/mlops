import os
import duckdb
from contextlib import contextmanager
from typing import Generator


@contextmanager
def get_connection(env: str) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    if "MOTHERDUCK_TOKEN" not in os.environ:
        raise EnvironmentError("MOTHERDUCK_TOKEN is not set")
    db_name = f"warehouse_{env}"
    con = duckdb.connect("md:")
    con.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    con.execute(f"USE {db_name}")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    try:
        yield con
    finally:
        con.close()
