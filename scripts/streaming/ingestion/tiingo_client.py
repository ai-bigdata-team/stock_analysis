import json
import time
import threading
import logging
from typing import List

import websocket

from ingestion.kafka_producer import send_message
from config.market_message import create_tiingo_message
from config.settings import settings
from config.kafka_config import kafka_config

logger = logging.getLogger(__name__)

TIINGO_WS_URL = settings.TIINGO_WS_URL   # ví dụ: wss://api.tiingo.com/iex
API_KEY = settings.TIINGO_API_KEY

RECONNECT_DELAY = 5  # seconds
MAX_RUNTIME = 600    # 10 minutes


def _run_ws(symbols: List[str]):
    """
    Internal WS runner (blocking).
    """

    def on_message(ws, message):
        try:
            msg = json.loads(message)

            # Tiingo có nhiều event type
            # trade / quote / heartbeat / error
            event_type = msg.get("messageType")

            if event_type not in ("I", "H"):  # Aggregate / Trade / Quote
                market_msg = create_tiingo_message(msg)
                send_message(
                    kafka_config.TOPICS["market_data"],
                    market_msg.to_dict()
                )

        except Exception as e:
            logger.exception("Error processing Tiingo message: %s", e)

    def on_open(ws):
        subscribe = {
            "eventName": "subscribe",
            "authorization": API_KEY,
            "eventData": {
                "tickers": symbols
            }
        }
        ws.send(json.dumps(subscribe))
        logger.info("Subscribed to Tiingo symbols: %s", symbols)

    def on_error(ws, error):
        logger.error("Tiingo WS error: %s", error)

    def on_close(ws, close_status_code, close_msg):
        logger.warning(
            "Tiingo WS closed: code=%s msg=%s",
            close_status_code,
            close_msg
        )

    ws = websocket.WebSocketApp(
        TIINGO_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever(
        ping_interval=20,
        ping_timeout=10,
        reconnect=0
    )


def start_tiingo(symbols: List[str]):
    """
    Blocking call.
    Ví dụ:
        start_tiingo(["AAPL", "MSFT"])
    """

    while True:
        try:
            logger.info("Starting Tiingo WebSocket client")
            _run_ws(symbols)
        except Exception as e:
            logger.exception("Tiingo WS connection error: %s", e)

        logger.info("Reconnect in %s seconds...", RECONNECT_DELAY)
        time.sleep(RECONNECT_DELAY)
