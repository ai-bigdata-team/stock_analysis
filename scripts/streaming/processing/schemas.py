from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class MarketMessageModel(BaseModel):
    message_id: str
    source: str
    data_type: str
    symbol: str
    timestamp: datetime
    price: Optional[float] = None
    volume: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    raw_data: Optional[Dict[str, Any]] = Field(default_factory=dict)
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)