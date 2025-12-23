import logging
import threading
from ingestion.finnhub_client import start_finnhub
# from ingestion.vnstock_client import start_vnstock_polling
from config.settings import settings
from processing.flink_job_local import kafka_to_bigquery_job
from vnstock import Vnstock

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    kafka_to_bigquery_job()