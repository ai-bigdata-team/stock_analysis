from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Dict
from ..config.bigquery_config import bigquery_config
from ..config.settings import settings
from pyflink.datastream.functions import MapFunction
from .parser import serialize_for_bigquery
import os
import logging

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

class BigQuerySink:
    def __init__(self):
        if not bigquery_config.USE_BQ:
            raise RuntimeError("BigQuerySink disabled in config")
        
        # Path tuyệt đối tới file JSON service account
        PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        key_path = os.path.join(PROJECT_ROOT, "keys", "bigquery-account.json")
        credentials = service_account.Credentials.from_service_account_file(key_path)

        self.client = bigquery.Client(
            project=bigquery_config.PROJECT_ID,
            credentials=credentials
        )
        self.table_id = f"{bigquery_config.PROJECT_ID}.{bigquery_config.DATASET}.{bigquery_config.TABLE}"

    def insert(self, msg: Dict):
        errors = self.client.insert_rows_json(self.table_id, [msg])
        if errors:
            print("BQ insert errors:", errors)

class BigQuerySinkMapFunction(MapFunction):
    def open(self, runtime_context):
        # Khởi tạo BigQuery client riêng cho worker
        PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        key_path = os.path.join(PROJECT_ROOT, "keys", "bigquery-account.json")
        credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = bigquery.Client(
            project=bigquery_config.PROJECT_ID,
            credentials=credentials
        )
        self.table_id = f"{bigquery_config.PROJECT_ID}.{bigquery_config.DATASET}.{bigquery_config.TABLE}"

    def map(self, value):
        # Serialize datetime trước khi insert
        value_serialized = serialize_for_bigquery(value)
        errors = self.client.insert_rows_json(self.table_id, [value_serialized])
        if errors:
            logger.error("BQ insert errors: %s", errors)
        logger.info("Inserted into BigQuery: %s", value_serialized)
        return value_serialized