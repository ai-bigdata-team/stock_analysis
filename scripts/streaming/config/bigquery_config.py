import os

class BigQueryConfig:
    PROJECT_ID = os.getenv("BQ_PROJECT_ID", "stockanalysis-480013")
    DATASET = os.getenv("BQ_DATASET", "market_data")
    TABLE = os.getenv("BQ_TABLE", "trades")
    USE_BQ = os.getenv("ENABLE_BIGQUERY", "true").lower() == "true"


bigquery_config = BigQueryConfig()
