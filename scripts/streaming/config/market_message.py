import json
import uuid
from datetime import datetime
from typing import Optional, Dict, Any


class MarketMessage:
    """
    Unified message schema for ALL data sources.
    """

    def __init__(self, source: str, data_type: str, symbol: str, **extra):
        self.message_id = str(uuid.uuid4())
        self.source = source
        self.data_type = data_type
        self.symbol = symbol
        self.timestamp = datetime.utcnow().isoformat()

        # Expected fields (optional)
        self.price: Optional[float] = extra.get("price")
        self.volume: Optional[float] = extra.get("volume")
        self.open: Optional[float] = extra.get("open")
        self.high: Optional[float] = extra.get("high")
        self.low: Optional[float] = extra.get("low")
        self.close: Optional[float] = extra.get("close")

        # raw + metadata
        self.raw_data: Dict[str, Any] = extra.get("raw_data", {})
        self.metadata: Dict[str, Any] = extra.get("metadata", {})

    def to_dict(self):
        """Convert message to dictionary (for Kafka)."""
        return {
            "schema_version": "1.0",
            "message_id": self.message_id,
            "source": self.source,
            "data_type": self.data_type,
            "symbol": self.symbol,
            "timestamp": self.timestamp,

            "data": {
                "price": self.price,
                "volume": self.volume,
                "open": self.open,
                "high": self.high,
                "low": self.low,
                "close": self.close,
            },

            "raw": self.raw_data,
            "meta": self.metadata,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


# Helpers
def create_finnhub_message(data: dict) -> MarketMessage:
    """Convert Finnhub WebSocket trade data → MarketMessage."""
    return MarketMessage(
        source="finnhub",
        data_type="trade",
        symbol=data.get("s", ""),
        price=data.get("p"),
        volume=data.get("v"),
        raw_data=data,
        metadata={
            "exchange": data.get("c", ""),
            "trade_id": data.get("t"),
        }
    )
