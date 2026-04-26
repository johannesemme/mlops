from datetime import datetime

import pandas as pd


def table_exists(con, schema: str, table_name: str) -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, table_name],
    ).fetchone()[0] > 0


def get_latest_timestamp(con, table_path: str, ts_col: str) -> datetime | None:
    return con.execute(f"SELECT MAX({ts_col}) FROM {table_path}").fetchone()[0]


def full_load(con, table_path: str, df: pd.DataFrame) -> None:
    con.begin()
    try:
        con.execute(f"DROP TABLE IF EXISTS {table_path}")
        con.execute(f"CREATE TABLE {table_path} AS SELECT * FROM df")
        con.commit()
    except Exception:
        con.rollback() # In case of an error, rollback the transaction to maintain data integrity
        raise


def incremental_load(con, table_path: str, df: pd.DataFrame, dedup_keys: list[str]) -> int:
    if df.empty:
        return 0

    key_condition = " AND ".join([f"df.{k} = existing.{k}" for k in dedup_keys])

    new_count = con.execute(f"""
        SELECT COUNT(*) FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM {table_path} existing
            WHERE {key_condition}
        )
    """).fetchone()[0]

    if new_count == 0:
        return 0

    con.begin()
    try:
        con.execute(f"""
            INSERT INTO {table_path}
            SELECT * FROM df
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_path} existing
                WHERE {key_condition}
            )
        """)
        con.commit()
    except Exception:
        con.rollback() # In case of an error, rollback the transaction to maintain data integrity
        raise

    return new_count
