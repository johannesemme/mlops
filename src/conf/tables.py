from dataclasses import dataclass, field

import conf.constants as cons


@dataclass(frozen=True)
class DBTable:
    schema: str
    table_name: str
    dtypes: dict[str, str] = field(default_factory=dict)

    @property
    def path(self) -> str:
        return f"{self.schema}.{self.table_name}"


# ---------------------------------------------------------
# Bronze
# ---------------------------------------------------------
GENERATION_AND_EXCHANGE_BRONZE = DBTable(
    schema="bronze",
    table_name="generation_and_exchange",
    dtypes={
        cons.TIME_UTC: "datetime64[us, UTC]",
        cons.TIME_DK: "datetime64[us]",
        cons.PRICE_AREA: "str",
        cons.VERSION: "str",
        cons.GROSS_CON_MWH: "float64",
        cons.CO2_PER_KWH: "float64",
    },
)

# ---------------------------------------------------------
# Silver
# ---------------------------------------------------------
GROSS_CONSUMPTION_SILVER = DBTable(
    schema="silver",
    table_name="gross_consumption",
    dtypes={
        cons.TIME_UTC: "datetime64[us, UTC]",
        cons.TIME_DK: "datetime64[us]",
        cons.PRICE_AREA: "str",
        cons.VERSION: "str",
        cons.GROSS_CON_MWH: "float64",
    },
)
