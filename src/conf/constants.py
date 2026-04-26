import os

# ENV controls which warehouse file is used: warehouse_{env}.duckdb
# Set the ENV environment variable to switch between dev/staging/prod.
ENV = os.getenv("ENV", "dev")

API_URL_GENERATION_AND_EXCHANGE = "https://api.energidataservice.dk/dataset/GenerationProdTypeExchange"

# Database load modes
LOAD_MODE_FULL = "full"
LOAD_MODE_INCREMENTAL = "incremental"
LOAD_MODE_BACKFILL = "backfill"

# Column names
TIME_UTC = "TimeUTC"
TIME_DK = "TimeDK"
PRICE_AREA = "PriceArea"
VERSION = "Version"
GROSS_CON = "GrossCon"
GROSS_CON_MWH = "GrossConsumptionMWh"
# OFFSHORE_WIND_POWER = "OffshoreWindPower"
# ONSHORE_WIND_POWER = "OnshoreWindPower"
# HYDRO_POWER = "HydroPower"
# SOLAR_POWER = "SolarPower"
# SOLAR_POWER_SELF_CON = "SolarPowerSelfCon"
# BIOMASS = "Biomass"
# BIOGAS = "Biogas"
# WASTE = "Waste"
# FOSSIL_GAS = "FossilGas"
# FOSSIL_OIL = "FossilOil"
# FOSSIL_HARD_COAL = "FossilHardCoal"
# EXCHANGE_GREAT_BELT = "ExchangeGreatBelt"
# EXCHANGE_GERMANY = "ExchangeGermany"
# EXCHANGE_SWEDEN = "ExchangeSweden"
# EXCHANGE_NORWAY = "ExchangeNorway"
# EXCHANGE_NETHERLANDS = "ExchangeNetherlands"
# EXCHANGE_GREAT_BRITAIN = "ExchangeGreatBritain"
CO2_PER_KWH = "CO2PerkWh"

# Version values
# VERSION_INITIAL = "Initial"
# VERSION_PRELIMINARY = "Preliminary"
# VERSION_FINAL = "Final"