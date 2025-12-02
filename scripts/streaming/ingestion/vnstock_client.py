import requests
import time
import logging
from .kafka_producer import send_message
from ..config.market_message import MarketMessage
from ..config.settings import settings
from ..config.kafka_config import kafka_config

logger = logging.getLogger(__name__)

BASE_URL = "https://api.vnstock.io"  # ví dụ

def fetch_vnstock(symbol: str):
    try:
        resp = requests.get(f"{BASE_URL}/quote/{symbol}")
        resp.raise_for_status()
        data = resp.json()
        msg = MarketMessage(
            source="vnstock",
            data_type="quote",
            symbol=symbol,
            price=data.get("price"),
            open=data.get("open"),
            high=data.get("high"),
            low=data.get("low"),
            close=data.get("close"),
            volume=data.get("volume"),
            raw_data=data,
        )
        send_message(kafka_config.TOPICS["market_data"], msg.to_dict())
    except Exception as e:
        logger.exception("VNStock fetch failed for %s: %s", symbol, e)

def start_vnstock_polling(symbols: list[str], interval_sec: int = 5):
    while True:
        for s in symbols:
            fetch_vnstock(s)
        time.sleep(interval_sec)
