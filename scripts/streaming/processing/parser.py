from processing.schemas import MarketMessageModel
import datetime
import json

def parse_market_message(msg_str: str) -> MarketMessageModel:
    """
    Parse Kafka JSON → MarketMessageModel
    Flatten nested fields (data/meta) and convert timestamp
    """
    try:
        data = json.loads(msg_str)
    except Exception as e:
        raise ValueError(f"Invalid JSON: {e}")

    payload = {
        "message_id": data.get("message_id"),
        "source": data.get("source"),
        "data_type": data.get("data_type"),
        "symbol": data.get("symbol"),
        "timestamp": data.get("timestamp"),
        "price": data.get("data", {}).get("price"),
        "volume": data.get("data", {}).get("volume"),
        "open": data.get("data", {}).get("open"),
        "high": data.get("data", {}).get("high"),
        "low": data.get("data", {}).get("low"),
        "raw_data": data.get("raw", {}),
        "metadata": data.get("meta", {}),
        "close": data.get("data", {}).get("close"),

    }

    # Pydantic validation
    return MarketMessageModel(**payload)

def serialize_for_bigquery(msg: dict):
    result = {}
    for k, v in msg.items():
        if isinstance(v, datetime.datetime):
            result[k] = v.isoformat()
        elif isinstance(v, datetime.date):
            result[k] = v.isoformat()
        elif isinstance(v, dict) and k in ["metadata", "raw_data"]:
            # convert dict -> JSON string để phù hợp schema JSON trên BQ
            result[k] = json.dumps(v)
        else:
            result[k] = v
    return result