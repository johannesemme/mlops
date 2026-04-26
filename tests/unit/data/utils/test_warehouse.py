import duckdb
import pandas as pd
import pytest

from data.utils.warehouse import full_load, get_latest_timestamp, incremental_load, table_exists


@pytest.fixture
def con():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    yield con
    con.close()


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "TimeUTC": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 01:00"], utc=True),
        "PriceArea": ["DK1", "DK1"],
        "Value": [100.0, 200.0],
    })


def test_table_exists_false_when_missing(con):
    assert table_exists(con, "bronze", "test_table") is False


def test_table_exists_true_after_creation(con, sample_df):
    full_load(con, "bronze.test_table", sample_df)
    assert table_exists(con, "bronze", "test_table") is True


def test_full_load_writes_all_rows(con, sample_df):
    full_load(con, "bronze.test_table", sample_df)
    count = con.execute("SELECT COUNT(*) FROM bronze.test_table").fetchone()[0]
    assert count == 2


def test_full_load_replaces_existing_data(con, sample_df):
    full_load(con, "bronze.test_table", sample_df)
    new_df = sample_df.iloc[:1]
    full_load(con, "bronze.test_table", new_df)
    count = con.execute("SELECT COUNT(*) FROM bronze.test_table").fetchone()[0]
    assert count == 1


def test_get_latest_timestamp(con, sample_df):
    full_load(con, "bronze.test_table", sample_df)
    last_ts = get_latest_timestamp(con, "bronze.test_table", "TimeUTC")
    assert last_ts == pd.Timestamp("2024-01-01 01:00", tz="UTC")


def test_get_latest_timestamp_returns_none_on_empty_table(con, sample_df):
    full_load(con, "bronze.test_table", sample_df)
    con.execute("DELETE FROM bronze.test_table")
    assert get_latest_timestamp(con, "bronze.test_table", "TimeUTC") is None


def test_incremental_load_inserts_new_rows(con, sample_df):
    full_load(con, "bronze.test_table", sample_df)
    new_df = pd.DataFrame({
        "TimeUTC": pd.to_datetime(["2024-01-01 02:00"], utc=True),
        "PriceArea": ["DK1"],
        "Value": [300.0],
    })
    n = incremental_load(con, "bronze.test_table", new_df, ["TimeUTC", "PriceArea"])
    assert n == 1
    assert con.execute("SELECT COUNT(*) FROM bronze.test_table").fetchone()[0] == 3


def test_incremental_load_skips_duplicates(con, sample_df):
    full_load(con, "bronze.test_table", sample_df)
    n = incremental_load(con, "bronze.test_table", sample_df, ["TimeUTC", "PriceArea"])
    assert n == 0
    assert con.execute("SELECT COUNT(*) FROM bronze.test_table").fetchone()[0] == 2


def test_incremental_load_empty_df_returns_zero(con, sample_df):
    full_load(con, "bronze.test_table", sample_df)
    empty_df = sample_df.iloc[0:0]
    n = incremental_load(con, "bronze.test_table", empty_df, ["TimeUTC", "PriceArea"])
    assert n == 0
