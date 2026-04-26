import conf.constants as cons
from dagster import Config, Definitions, ScheduleDefinition, asset, define_asset_job

from data.bronze.generation_and_exchange_bronze import run as run_generation_and_exchange
from data.silver.gross_consumption_silver import run as run_gross_power_consumption


class GenerationAndExchangeConfig(Config):
    mode: str = cons.LOAD_MODE_INCREMENTAL
    start_utc: str = ""
    end_utc: str = ""


class SilverConfig(Config):
    mode: str = cons.LOAD_MODE_INCREMENTAL
    start_utc: str = ""
    end_utc: str = ""


##############################
#          Assets
##############################

@asset
def generation_and_exchange_bronze(config: GenerationAndExchangeConfig) -> None:
    run_generation_and_exchange(
        mode=config.mode,
        start_utc=config.start_utc or None,
        end_utc=config.end_utc or None,
    )


@asset(deps=[generation_and_exchange_bronze])
def gross_power_consumption_silver(config: SilverConfig) -> None:
    run_gross_power_consumption(
        mode=config.mode,
        start_utc=config.start_utc or None,
        end_utc=config.end_utc or None,
    )


##############################
#          Schedules
##############################

bronze_schedule = ScheduleDefinition(
    job=define_asset_job("pipeline_job", selection=[generation_and_exchange_bronze, gross_power_consumption_silver]),
    cron_schedule="0 * * * *",  # hourly
)

##############################
#          Definitions
##############################

defs = Definitions(
    assets=[generation_and_exchange_bronze, gross_power_consumption_silver],
    schedules=[bronze_schedule],
)
