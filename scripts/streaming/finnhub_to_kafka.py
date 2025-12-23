import logging
import threading
from ingestion.finnhub_client import start_finnhub
# from ingestion.vnstock_client import start_vnstock_polling
from config.settings import settings
from config.kafka_admin import create_topic_if_not_exists

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    create_topic_if_not_exists(settings.KAFKA_TOPIC, num_partitions=3, replication_factor=1)
    # # VNStock polling in background thread
    # vn_symbols = ["AAPL", "MSFT"]  # ví dụ
    # vn_thread = threading.Thread(target=start_vnstock_polling, args=(vn_symbols,), daemon=True)
    # vn_thread.start()

    # Finnhub WebSocket (blocking)
    logger.info("Starting Finnhub client")
    start_finnhub(symbols=settings.STOCKCODE)