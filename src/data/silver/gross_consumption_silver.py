import conf.constants as cons
from conf.tables import GENERATION_AND_EXCHANGE_BRONZE, GROSS_CONSUMPTION_SILVER
from data.db import get_connection
from data.utils.warehouse import full_load, get_latest_timestamp, incremental_load, table_exists
from data.validation.dq_checking import data_quality_check_silver_df
from utils.logger import get_logger

logger = get_logger("silver.gross_power_consumption")


def _normalize(df):
    df[cons.TIME_UTC] = df[cons.TIME_UTC].dt.tz_convert("UTC")
    return df


def run(
    mode: str = cons.LOAD_MODE_INCREMENTAL,
    start_utc: str | None = None,
    end_utc: str | None = None,
) -> None:
    env = cons.ENV
    source_path = GENERATION_AND_EXCHANGE_BRONZE.path
    target_path = GROSS_CONSUMPTION_SILVER.path

    with get_connection(env) as con:
        if mode == cons.LOAD_MODE_FULL:
            df = _normalize(con.execute(f"""
                SELECT TimeUTC, TimeDK, PriceArea, Version, GrossConsumptionMWh
                FROM {source_path}
                ORDER BY TimeUTC, PriceArea
            """).df())
            data_quality_check_silver_df(df)
            full_load(con, target_path, df)
            logger.info("Full load: %d rows written to %s", len(df), target_path)

        elif mode == cons.LOAD_MODE_INCREMENTAL:
            if not table_exists(con, GROSS_CONSUMPTION_SILVER.schema, GROSS_CONSUMPTION_SILVER.table_name):
                if not start_utc or not end_utc:
                    raise ValueError("incremental on a new table requires start_utc and end_utc for the initial full load")
                df = _normalize(con.execute(f"""
                    SELECT TimeUTC, TimeDK, PriceArea, Version, GrossConsumptionMWh
                    FROM {source_path}
                    WHERE TimeUTC BETWEEN ? AND ?
                    ORDER BY TimeUTC, PriceArea
                """, [start_utc, end_utc]).df())
                data_quality_check_silver_df(df)
                full_load(con, target_path, df)
                logger.info("Full load (first run): %d rows written to %s", len(df), target_path)
            else:
                latest_ts = get_latest_timestamp(con, target_path, cons.TIME_UTC)
                df = _normalize(con.execute(f"""
                    SELECT TimeUTC, TimeDK, PriceArea, Version, GrossConsumptionMWh
                    FROM {source_path}
                    WHERE TimeUTC > ?
                    ORDER BY TimeUTC, PriceArea
                """, [latest_ts]).df())
                if df.empty:
                    logger.info("Incremental load: no new rows since %s", latest_ts)
                    return
                data_quality_check_silver_df(df)
                n = incremental_load(con, target_path, df, dedup_keys=[cons.TIME_UTC, cons.PRICE_AREA])
                logger.info("Incremental load: %d new rows written to %s", n, target_path)

        elif mode == cons.LOAD_MODE_BACKFILL:
            if not start_utc or not end_utc:
                raise ValueError("backfill requires both start_utc and end_utc")
            df = _normalize(con.execute(f"""
                SELECT TimeUTC, TimeDK, PriceArea, Version, GrossConsumptionMWh
                FROM {source_path}
                WHERE TimeUTC BETWEEN ? AND ?
                ORDER BY TimeUTC, PriceArea
            """, [start_utc, end_utc]).df())
            data_quality_check_silver_df(df)
            n = incremental_load(con, target_path, df, dedup_keys=[cons.TIME_UTC, cons.PRICE_AREA])
            logger.info("Backfill: %d new rows loaded for %s → %s", n, start_utc, end_utc)

        else:
            raise ValueError(f"Unknown mode '{mode}'. Use 'full', 'incremental', or 'backfill'.")
