from collections.abc import Callable
import pandas as pd
from conf import constants as cons

SOURCE_PROCESSOR_REGISTRY: dict[str, Callable] = {}


def XXX(df: pd.DataFrame) -> pd.DataFrame:
    return df[df[cons.YYY] == "yyy"]