import asyncio
import websockets
import json
import logging
from typing import List
from ingestion.kafka_producer import send_message
from config.market_message import create_finnhub_message
from config.settings import settings
from config.kafka_config import kafka_config

logger = logging.getLogger(__name__)

RECONNECT_DELAY = 5  # giây

async def _subscribe(symbols: List[str]):
    url = f"{settings.FINNHUB_WS_URL}?token={settings.FINNHUB_API_KEY}"
    while True:
        try:
            async with websockets.connect(url) as ws:
                # Subscribe tất cả symbol
                for sym in symbols:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
                    logger.info("Subscribed to Finnhub symbol: %s", sym)

                # Lắng nghe tin trade
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("type") == "trade":
                        for t in data.get("data", []):
                            market_msg = create_finnhub_message(t)
                            send_message(kafka_config.TOPICS["market_data"], market_msg.to_dict())
        except Exception as e:
            logger.exception("Finnhub WS connection error: %s", e)
            logger.info("Reconnect in %s seconds...", RECONNECT_DELAY)
            await asyncio.sleep(RECONNECT_DELAY)

def start_finnhub(symbols: List[str]):
    """
    Blocking call, truyền vào danh sách symbol để subscribe.
    Ví dụ: start_finnhub(["AAPL","MSFT"])
    """
    asyncio.get_event_loop().run_until_complete(_subscribe(symbols))