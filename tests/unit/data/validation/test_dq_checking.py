import pandas as pd
import pytest

from data.validation.dq_checking import data_quality_check_bronze_df

_SCHEMA = {
    "TimeUTC": "datetime64[us, UTC]",
    "TimeDK": "datetime64[us]",
    "PriceArea": "str",
    "Version": "str",
    "GrossConsumptionMWh": "float64",
}


def _valid_df():
    return pd.DataFrame({
        "TimeUTC": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 01:00"], utc=True),
        "TimeDK": pd.to_datetime(["2024-01-01 01:00", "2024-01-01 02:00"]),
        "PriceArea": ["DK1", "DK2"],
        "Version": ["Initial", "Final"],
        "GrossConsumptionMWh": [100.0, 200.0],
    })


def test_valid_df_passes():
    data_quality_check_bronze_df(_valid_df(), _SCHEMA)


def test_missing_column_raises():
    df = _valid_df().drop(columns=["PriceArea"])
    with pytest.raises(ValueError, match="Missing required columns"):
        data_quality_check_bronze_df(df, _SCHEMA)


def test_wrong_dtype_raises():
    df = _valid_df()
    df["GrossConsumptionMWh"] = df["GrossConsumptionMWh"].astype(str)
    with pytest.raises(ValueError, match="Column dtype mismatch"):
        data_quality_check_bronze_df(df, _SCHEMA)
