import pandas as pd

import conf.constants as cons
from conf.tables import GROSS_CONSUMPTION_SILVER


def data_quality_check_bronze_df(df: pd.DataFrame, dtypes: dict[str, str]) -> None:
    missing = [c for c in dtypes if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    wrong_types = {
        col: {"expected": expected, "got": str(df[col].dtype)}
        for col, expected in dtypes.items()
        if str(df[col].dtype) != expected
    }
    if wrong_types:
        raise ValueError(f"Column dtype mismatch: {wrong_types}")


def data_quality_check_silver_df(df: pd.DataFrame) -> None:
    dtypes = GROSS_CONSUMPTION_SILVER.dtypes

    # 1. Schema check
    missing = [c for c in dtypes if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    wrong_types = {
        col: {"expected": expected, "got": str(df[col].dtype)}
        for col, expected in dtypes.items()
        if str(df[col].dtype) != expected
    }
    if wrong_types:
        raise ValueError(f"Column dtype mismatch: {wrong_types}")

    # 2. No nulls in any column
    nulls = {c: int(df[c].isna().sum()) for c in df.columns if df[c].isna().any()}
    if nulls:
        raise ValueError(f"Null values found: {nulls}")

    # 3. Value bounds for GrossConsumptionMWh
    col = cons.GROSS_CON_MWH
    if (df[col] < 0).any():
        raise ValueError(f"{col} contains negative values (min={df[col].min()})")
    if (df[col] > 15_000).any():
        raise ValueError(f"{col} exceeds 15,000 MWh (max={df[col].max()})")

    # 4. Hourly completeness per PriceArea
    for area, group in df.groupby(cons.PRICE_AREA):
        times = group[cons.TIME_UTC].sort_values().reset_index(drop=True)
        if len(times) < 2:
            continue
        diffs = times.diff().dropna()
        expected = pd.Timedelta("1h")
        gaps = diffs[diffs != expected]
        if not gaps.empty:
            gap_times = times[gaps.index].tolist()
            raise ValueError(
                f"Hourly gaps in TimeUTC for PriceArea={area!r}: "
                f"{len(gaps)} gap(s), first at {gap_times[0]}"
            )
