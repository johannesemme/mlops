from typing import Callable, Dict, List, Optional
import pandas as pd
from src.ml.loaders.dataloader_functions import SOURCE_PROCESSOR_REGISTRY


class DuckDBDataLoader:
    def __init__(
        self,
        table_path: str,
        processing_functions: Optional[List[Callable]] = None,
    ):
        self.table_path = table_path
        self.processing_functions = processing_functions or []

    def load(self, date_col: str, start_date_utc: str, end_date_utc: str) -> pd.DataFrame:
        # Verify that the date range is valid before trying to load data
        if end_date_utc < start_date_utc:
            raise ValueError(
                f"end_date_utc ({end_date_utc}) must be >= start_date_utc ({start_date_utc})"
            )
        
        # Load data to pandas DataFrame
        spark = Spark.get()
        dfp = spark.read.table(self.table_path).filter(
            (col(date_col) >= start_date_utc) & (col(date_col) <= end_date_utc)
        ).toPandas()

        # Apply each of the processing functions in sequence
        for fn in self.processing_functions:
            dfp = fn(dfp)

        # Local time needed for downstream processing
        return self.add_local_time(dfp) 
        

        
    def to_config(self) -> Dict:
        """ Convert the DBDataLoader instance to a config dictionary that can be used to recreate it."""
        return {
            "table_path": self.table_path,
            "processing_functions": [f.__name__ for f in self.processing_functions],
        }
    
    @classmethod
    def from_config(cls, config: Dict) -> "DBDataLoader":
        """ Create a DBDataLoader instance from a config dictionary."""

        # Resolve the processing function names to actual functions using the registry
        names = config.get("processing_functions", [])
        missing = [n for n in names if n not in SOURCE_PROCESSOR_REGISTRY]
        if missing:
            raise ValueError(f"Unknown source processors: {missing}")
        
        # Create the DBDataLoader instance 
        return cls(
            table_path=config["table_path"],
            processing_functions=[SOURCE_PROCESSOR_REGISTRY[n] for n in names],
        )