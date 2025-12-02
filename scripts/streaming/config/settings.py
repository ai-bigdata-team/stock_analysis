import os
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
dotenv_path = os.path.join(PROJECT_ROOT, ".env")

load_dotenv(dotenv_path)

class Settings:
    # Kafka
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market_stream")

    # Finnhub
    FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
    FINNHUB_WS_URL = os.getenv("FINNHUB_WS_URL", "wss://ws.finnhub.io")

    # VNStock (optional)
    VNSTOCK_ENABLED = os.getenv("VNSTOCK_ENABLED", "false").lower() == "true"

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # App
    APP_NAME = "market-data-pipeline"

    FLINK_STANDALONE = os.getenv("FLINK_STANDALONE", "true").lower() == "true"
    # BigQuery
    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "")
    BQ_DATASET = os.getenv("BQ_DATASET", "market_data")
    BQ_TABLE = os.getenv("BQ_TABLE", "trades")
    ENABLE_BIGQUERY = os.getenv("ENABLE_BIGQUERY", "true").lower() == "true"


settings = Settings()