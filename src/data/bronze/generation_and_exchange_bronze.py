from datetime import datetime, timezone

import pandas as pd

import conf.constants as cons
from conf.tables import GENERATION_AND_EXCHANGE_BRONZE
from data.db import get_connection
from data.utils.api import fetch_records_as_pdf
from data.utils.warehouse import full_load, get_latest_timestamp, incremental_load, table_exists
from data.validation.dq_checking import data_quality_check_bronze_df
from utils.logger import get_logger

logger = get_logger("bronze.generation_and_exchange")


def _fetch(start_utc: str | None = None, end_utc: str | None = None) -> pd.DataFrame:
    params: dict = {"sort": f"{cons.TIME_UTC} DESC"}
    if start_utc:
        params["start"] = start_utc
    if end_utc:
        params["end"] = end_utc

    df = fetch_records_as_pdf(cons.API_URL_GENERATION_AND_EXCHANGE, params=params)
    if df.empty:
        raise ValueError(f"API returned no records for params: {params}")
    return df


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    # Fix data types
    df[cons.TIME_UTC] = pd.to_datetime(df[cons.TIME_UTC], utc=True)
    df[cons.TIME_DK] = pd.to_datetime(df[cons.TIME_DK])
    
    # Rename columns
    df.rename(columns={cons.GROSS_CON: cons.GROSS_CON_MWH}, inplace=True)
    
    # Add load timestamp
    df["_loaded_at"] = datetime.now(timezone.utc)
    
    # Select only relevant columns 
    column_names = list(GENERATION_AND_EXCHANGE_BRONZE.dtypes.keys())
    df = df[column_names + ["_loaded_at"]]
    return df


def run(
    mode: str = cons.LOAD_MODE_INCREMENTAL,
    start_utc: str | None = None,
    end_utc: str | None = None,
) -> None:
    env = cons.ENV
    target_path = GENERATION_AND_EXCHANGE_BRONZE.path

    with get_connection(env) as con:
        if mode == cons.LOAD_MODE_FULL or not table_exists(con, GENERATION_AND_EXCHANGE_BRONZE.schema, GENERATION_AND_EXCHANGE_BRONZE.table_name):
            if not start_utc or not end_utc:
                raise ValueError("full load requires both start_utc and end_utc (YYYY-MM-DD)")
            df = _fetch(start_utc=start_utc, end_utc=end_utc)
            df = _transform(df)
            data_quality_check_bronze_df(df, GENERATION_AND_EXCHANGE_BRONZE.dtypes)
            full_load(con, target_path, df)
            logger.info("Full load: %d rows written to %s", len(df), target_path)

        elif mode == cons.LOAD_MODE_INCREMENTAL:
            last_ts = get_latest_timestamp(con, target_path, cons.TIME_UTC)
            start_utc = last_ts.strftime("%Y-%m-%dT%H:%M") if last_ts else None
            end_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
            df = _fetch(start_utc=start_utc, end_utc=end_utc)
            df = _transform(df)
            data_quality_check_bronze_df(df, GENERATION_AND_EXCHANGE_BRONZE.dtypes)
            n = incremental_load(con, target_path, df, [cons.TIME_UTC, cons.PRICE_AREA, cons.VERSION])
            logger.info("Incremental load: %d new rows appended to %s", n, target_path)

        elif mode == cons.LOAD_MODE_BACKFILL:
            if not start_utc or not end_utc:
                raise ValueError("backfill requires both start_utc and end_utc (YYYY-MM-DD)")
            df = _fetch(start_utc=start_utc, end_utc=end_utc)
            df = _transform(df)
            data_quality_check_bronze_df(df, GENERATION_AND_EXCHANGE_BRONZE.dtypes)
            n = incremental_load(con, target_path, df, [cons.TIME_UTC, cons.PRICE_AREA, cons.VERSION])
            logger.info("Backfill: %d new rows loaded for %s → %s", n, start_utc, end_utc)

        else:
            raise ValueError(f"Unknown mode '{mode}'. Use '{cons.LOAD_MODE_FULL}', '{cons.LOAD_MODE_INCREMENTAL}', or '{cons.LOAD_MODE_BACKFILL}'.")
