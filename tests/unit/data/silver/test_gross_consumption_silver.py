import duckdb
import pandas as pd
import pytest

import conf.constants as cons
from conf.tables import GENERATION_AND_EXCHANGE_BRONZE, GROSS_CONSUMPTION_SILVER
from data.utils.warehouse import full_load, get_latest_timestamp, incremental_load, table_exists


@pytest.fixture
def con():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    con.execute("CREATE SCHEMA silver")
    yield con
    con.close()


@pytest.fixture
def bronze_df():
    return pd.DataFrame({
        "TimeUTC": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 01:00", "2024-01-01 02:00"], utc=True),
        "TimeDK": pd.to_datetime(["2024-01-01 01:00", "2024-01-01 02:00", "2024-01-01 03:00"]),
        "PriceArea": ["DK1", "DK1", "DK1"],
        "Version": ["Final", "Final", "Final"],
        "GrossConsumptionMWh": [1000.0, 1100.0, 1200.0],
        "CO2PerkWh": [120.0, 130.0, 140.0],
    })


def _seed_bronze(con, df):
    full_load(con, GENERATION_AND_EXCHANGE_BRONZE.path, df)


def _run_silver(con, bronze_df, mode=cons.LOAD_MODE_INCREMENTAL, start_utc=None, end_utc=None):
    """Run silver logic inline against the in-memory connection (avoids needing MotherDuck)."""
    source_path = GENERATION_AND_EXCHANGE_BRONZE.path
    target_path = GROSS_CONSUMPTION_SILVER.path

    _seed_bronze(con, bronze_df)

    if mode == cons.LOAD_MODE_FULL:
        df = con.execute(f"""
            SELECT TimeUTC, TimeDK, PriceArea, Version, GrossConsumptionMWh
            FROM {source_path}
            ORDER BY TimeUTC, PriceArea
        """).df()
        full_load(con, target_path, df)
        return len(df)

    elif mode == cons.LOAD_MODE_INCREMENTAL:
        if not table_exists(con, GROSS_CONSUMPTION_SILVER.schema, GROSS_CONSUMPTION_SILVER.table_name):
            if not start_utc or not end_utc:
                raise ValueError("incremental on a new table requires start_utc and end_utc")
            df = con.execute(f"""
                SELECT TimeUTC, TimeDK, PriceArea, Version, GrossConsumptionMWh
                FROM {source_path}
                WHERE TimeUTC BETWEEN ? AND ?
                ORDER BY TimeUTC, PriceArea
            """, [start_utc, end_utc]).df()
            full_load(con, target_path, df)
            return len(df)
        else:
            latest_ts = get_latest_timestamp(con, target_path, cons.TIME_UTC)
            df = con.execute(f"""
                SELECT TimeUTC, TimeDK, PriceArea, Version, GrossConsumptionMWh
                FROM {source_path}
                WHERE TimeUTC > ?
                ORDER BY TimeUTC, PriceArea
            """, [latest_ts]).df()
            return incremental_load(con, target_path, df, dedup_keys=[cons.TIME_UTC, cons.PRICE_AREA])

    elif mode == cons.LOAD_MODE_BACKFILL:
        if not start_utc or not end_utc:
            raise ValueError("backfill requires start_utc and end_utc")
        df = con.execute(f"""
            SELECT TimeUTC, TimeDK, PriceArea, Version, GrossConsumptionMWh
            FROM {source_path}
            WHERE TimeUTC BETWEEN ? AND ?
            ORDER BY TimeUTC, PriceArea
        """, [start_utc, end_utc]).df()
        return incremental_load(con, target_path, df, dedup_keys=[cons.TIME_UTC, cons.PRICE_AREA])


# --- Full mode ---

def test_full_mode_loads_all_rows(con, bronze_df):
    count = _run_silver(con, bronze_df, mode=cons.LOAD_MODE_FULL)
    assert count == 3
    total = con.execute(f"SELECT COUNT(*) FROM {GROSS_CONSUMPTION_SILVER.path}").fetchone()[0]
    assert total == 3


def test_full_mode_includes_version_column(con, bronze_df):
    _run_silver(con, bronze_df, mode=cons.LOAD_MODE_FULL)
    cols = [r[0] for r in con.execute(f"DESCRIBE {GROSS_CONSUMPTION_SILVER.path}").fetchall()]
    assert "Version" in cols


def test_full_mode_replaces_existing_data(con, bronze_df):
    _run_silver(con, bronze_df, mode=cons.LOAD_MODE_FULL)
    single_row = bronze_df.iloc[:1].copy()
    _run_silver(con, single_row, mode=cons.LOAD_MODE_FULL)
    total = con.execute(f"SELECT COUNT(*) FROM {GROSS_CONSUMPTION_SILVER.path}").fetchone()[0]
    assert total == 1


# --- Incremental mode ---

def test_incremental_first_run_requires_dates(con, bronze_df):
    with pytest.raises(ValueError, match="start_utc and end_utc"):
        _run_silver(con, bronze_df, mode=cons.LOAD_MODE_INCREMENTAL)


def test_incremental_first_run_full_loads_with_dates(con, bronze_df):
    count = _run_silver(con, bronze_df, mode=cons.LOAD_MODE_INCREMENTAL,
                        start_utc="2024-01-01", end_utc="2024-01-02")
    assert count == 3


def test_incremental_inserts_only_new_rows(con, bronze_df):
    _run_silver(con, bronze_df, mode=cons.LOAD_MODE_INCREMENTAL,
                start_utc="2024-01-01", end_utc="2024-01-02")
    new_row = pd.DataFrame({
        "TimeUTC": pd.to_datetime(["2024-01-01 03:00"], utc=True),
        "TimeDK": pd.to_datetime(["2024-01-01 04:00"]),
        "PriceArea": ["DK1"],
        "Version": ["Final"],
        "GrossConsumptionMWh": [1300.0],
        "CO2PerkWh": [110.0],
    })
    extended = pd.concat([bronze_df, new_row], ignore_index=True)
    inserted = _run_silver(con, extended, mode=cons.LOAD_MODE_INCREMENTAL)
    assert inserted == 1
    total = con.execute(f"SELECT COUNT(*) FROM {GROSS_CONSUMPTION_SILVER.path}").fetchone()[0]
    assert total == 4


def test_incremental_no_new_rows_inserts_zero(con, bronze_df):
    _run_silver(con, bronze_df, mode=cons.LOAD_MODE_INCREMENTAL,
                start_utc="2024-01-01", end_utc="2024-01-02")
    inserted = _run_silver(con, bronze_df, mode=cons.LOAD_MODE_INCREMENTAL)
    assert inserted == 0


# --- Backfill mode ---

def test_backfill_requires_dates(con, bronze_df):
    _run_silver(con, bronze_df, mode=cons.LOAD_MODE_FULL)
    with pytest.raises(ValueError, match="start_utc and end_utc"):
        _run_silver(con, bronze_df, mode=cons.LOAD_MODE_BACKFILL)


def test_backfill_upserts_only_rows_in_range(con, bronze_df):
    _run_silver(con, bronze_df.iloc[:2], mode=cons.LOAD_MODE_FULL)
    inserted = _run_silver(con, bronze_df, mode=cons.LOAD_MODE_BACKFILL,
                           start_utc="2024-01-01 00:00", end_utc="2024-01-01 03:00")
    assert inserted == 1
    total = con.execute(f"SELECT COUNT(*) FROM {GROSS_CONSUMPTION_SILVER.path}").fetchone()[0]
    assert total == 3


def test_backfill_skips_existing_rows(con, bronze_df):
    _run_silver(con, bronze_df, mode=cons.LOAD_MODE_FULL)
    inserted = _run_silver(con, bronze_df, mode=cons.LOAD_MODE_BACKFILL,
                           start_utc="2024-01-01 00:00", end_utc="2024-01-01 03:00")
    assert inserted == 0
