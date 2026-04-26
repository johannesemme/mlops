# Data

Data-related modules for the MLOps pipeline — covering ingestion, quality checks, and feature preparation.

## Structure

```
data/
├── loaders/
│   └── dataloader.py        # load raw data from source systems
├── bronze/                  # (planned) ETL scripts — raw ingestion layer
├── silver/                  # (planned) ETL scripts — cleaned and enriched layer
├── validation/              # (planned) data quality checks
└── feature_store/           # (planned) feature table generation
```

## Modules

### `loaders/`
Responsible for loading data for the ML models.

### `bronze/` _(planned)_
ETL scripts for the raw ingestion layer. Data lands here with minimal transformation — schema enforcement and column selection only.

### `silver/` _(planned)_
ETL scripts for the cleaned and enriched layer. Applies business logic, joins, and transformations on top of bronze data.

### `validation/` _(planned)_
Data quality checking.

### `feature_store/` _(planned)_
Feature table generation for model training. Covers e.g. date/calendar features (hour, weekday, holidays, DST) and price-derived features. 
