from typing import Any, Callable, Dict, List, Optional
import pandas as pd
from ml.features.feature_functions import FEATURE_FUNCTION_REGISTRY


class Featurizer:





    def __init__(
        self,
        feature_functions: Optional[List[Callable]] = None,
        feature_tables: Optional[List[Dict]] = None,
        cols_to_drop: Optional[List[str]] = None,
    ):
        self.feature_functions = feature_functions or []
        self.feature_tables = feature_tables or []
        self.cols_to_drop = cols_to_drop if cols_to_drop is not None else [cols.DT_HOUR_LOCAL, cols.ZONE]

    def transform(self, df: pd.DataFrame) -> Any:
        df = self._get_features_from_store(df)
        df = self._apply_feature_functions(df)
        for col in self.cols_to_drop:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
        return df

    def _get_features_from_store(self, df: pd.DataFrame) -> pd.DataFrame:
        spark = Spark.get()
        for table in self.feature_tables:
            join_on = table.get("join_on", [cols.DT_HOUR])
            feature_df = (
                spark.read.table(table["table_path"])
                .select(*join_on, *table["columns"])
                .toPandas()
            )
            df = df.merge(feature_df, on=join_on, how="left")
        return df

    def _apply_feature_functions(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply each of the feature functions in sequence to the DataFrame. """
        for fn in self.feature_functions:
            df = fn(df)
        return df

    def to_config(self) -> Dict:
        """ Convert the Featurizer instance to a config dictionary that can be used to recreate it."""
        
        # Function used to serialize the feature functions, handling both regular functions and functools.partial objects
        def _serialize_fn(f) -> Dict:
            if hasattr(f, "func"):  # functools.partial
                return {
                    "name": f.func.__name__,
                    "keywords": f.keywords,
                    "args": list(f.args),
                }
            return {"name": f.__name__}

        return {
            "feature_functions": [_serialize_fn(f) for f in self.feature_functions],
            "feature_tables": self.feature_tables,
            "cols_to_drop": self.cols_to_drop,
        }

    @classmethod
    def from_config(cls, config: Dict) -> "Featurizer":
        from functools import partial

        # Resolve the feature function entries in the config to actual functions using the registry.
        # Handles both regular and partial functions.
        def _deserialize_fn(entry) -> Callable:
            name = entry if isinstance(entry, str) else entry["name"]
            if name not in FEATURE_FUNCTION_REGISTRY:
                raise ValueError(
                    f"Unknown feature function: '{name}'. Add it to FEATURE_FUNCTION_REGISTRY."
                )
            fn = FEATURE_FUNCTION_REGISTRY[name]
            if isinstance(entry, dict) and (entry.get("keywords") or entry.get("args")):
                fn = partial(fn, *entry.get("args", []), **entry.get("keywords", {}))
            return fn

        return cls(
            feature_functions=[_deserialize_fn(e) for e in config.get("feature_functions", [])],
            feature_tables=config.get("feature_tables", []),
            cols_to_drop=config.get("cols_to_drop", None),
        )
