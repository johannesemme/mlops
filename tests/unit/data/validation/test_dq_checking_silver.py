import pandas as pd
import pytest

from data.validation.dq_checking import data_quality_check_silver_df


def _valid_df(price_areas=("DK1", "DK2")):
    """Two complete hourly rows per PriceArea."""
    rows = []
    for area in price_areas:
        rows += [
            {"TimeUTC": pd.Timestamp("2024-01-01 00:00", tz="UTC"), "TimeDK": pd.Timestamp("2024-01-01 01:00"), "PriceArea": area, "Version": "Final", "GrossConsumptionMWh": 1000.0},
            {"TimeUTC": pd.Timestamp("2024-01-01 01:00", tz="UTC"), "TimeDK": pd.Timestamp("2024-01-01 02:00"), "PriceArea": area, "Version": "Final", "GrossConsumptionMWh": 1100.0},
        ]
    df = pd.DataFrame(rows)
    df["TimeUTC"] = pd.to_datetime(df["TimeUTC"], utc=True)
    df["TimeDK"] = pd.to_datetime(df["TimeDK"])
    df["GrossConsumptionMWh"] = df["GrossConsumptionMWh"].astype("float64")
    return df


def test_valid_df_passes():
    data_quality_check_silver_df(_valid_df())


def test_single_price_area_passes():
    data_quality_check_silver_df(_valid_df(price_areas=("DK1",)))


def test_missing_column_raises():
    df = _valid_df().drop(columns=["Version"])
    with pytest.raises(ValueError, match="Missing required columns"):
        data_quality_check_silver_df(df)


def test_wrong_dtype_raises():
    df = _valid_df()
    df["GrossConsumptionMWh"] = df["GrossConsumptionMWh"].astype(str)
    with pytest.raises(ValueError, match="Column dtype mismatch"):
        data_quality_check_silver_df(df)


def test_null_in_any_column_raises():
    df = _valid_df()
    df.loc[0, "GrossConsumptionMWh"] = None
    with pytest.raises(ValueError, match="Null values found"):
        data_quality_check_silver_df(df)


def test_null_in_time_utc_raises():
    df = _valid_df()
    df.loc[0, "TimeUTC"] = pd.NaT
    with pytest.raises(ValueError, match="Null values found"):
        data_quality_check_silver_df(df)


def test_negative_consumption_raises():
    df = _valid_df()
    df.loc[0, "GrossConsumptionMWh"] = -1.0
    with pytest.raises(ValueError, match="negative values"):
        data_quality_check_silver_df(df)


def test_consumption_above_max_raises():
    df = _valid_df()
    df.loc[0, "GrossConsumptionMWh"] = 20_000.0
    with pytest.raises(ValueError, match="exceeds 15,000"):
        data_quality_check_silver_df(df)


def test_hourly_gap_raises():
    df = _valid_df(price_areas=("DK1",))
    # Replace the second row with a timestamp 2 hours after the first (creating a gap)
    df.loc[1, "TimeUTC"] = pd.Timestamp("2024-01-01 02:00", tz="UTC")
    with pytest.raises(ValueError, match="Hourly gaps"):
        data_quality_check_silver_df(df)


def test_single_row_per_area_passes():
    """A single row per PriceArea has no diffs to check — should pass."""
    df = _valid_df().iloc[:1].copy()  # Only 1 row for DK1
    df = df.reset_index(drop=True)
    data_quality_check_silver_df(df)


def test_multiple_areas_gap_in_one_raises():
    """A gap in one PriceArea should raise even if the other is clean."""
    df = _valid_df(price_areas=("DK1", "DK2"))
    # Introduce a gap in DK2 only
    dk2_mask = df["PriceArea"] == "DK2"
    df.loc[df[dk2_mask].index[1], "TimeUTC"] = pd.Timestamp("2024-01-01 03:00", tz="UTC")
    with pytest.raises(ValueError, match="DK2"):
        data_quality_check_silver_df(df)
