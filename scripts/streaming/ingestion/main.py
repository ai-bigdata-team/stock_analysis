import logging
import threading
from .finnhub_client import start_finnhub
# from ingestion.vnstock_client import start_vnstock_polling
from ..config.settings import settings
from ..config.kafka_admin import create_topic_if_not_exists
from ...constant.stock_code_constant import STOCKCODE# , CRYPTO_CODE
import argparse 

'''
python -m scripts.streaming.ingestion.main --type-data crypto --topic crypto_stream
'''

def get_args():
    parser = argparse.ArgumentParser(description="Producer for Kafka")
    parser.add_argument("--type-data", choices=["stock", "crypto"], default = "stock",
                        help="Crawl stock data or crypto data")
    parser.add_argument("--topic", default=None, help="Kafka topics")

    args = parser.parse_args()
    return args

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    args = get_args()
    topic = args.topic if args.topic is not None else settings.KAFKA_TOPIC
    create_topic_if_not_exists(topic, num_partitions=3, replication_factor=1)
    # # VNStock polling in background thread
    # vn_symbols = ["AAPL", "MSFT"]  # ví dụ
    # vn_thread = threading.Thread(target=start_vnstock_polling, args=(vn_symbols,), daemon=True)
    # vn_thread.start()

    # Finnhub WebSocket (blocking)
    logger.info("Starting Finnhub client")
    code = STOCKCODE if args.type_data == "stock" else CRYPTO_CODE
    start_finnhub(symbols=code)