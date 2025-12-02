import os
import json
import time
import logging
import finnhub
import websocket
import pandas as pd
from kafka.producer import KafkaProducer
from ..constant import us_stock_code_constant as stock_code_constant
from typing import Union, Any
from dotenv import load_dotenv
load_dotenv()



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



KAFKA_NODES = os.getenv("KAFKA_NODES")
producer = KafkaProducer(bootstrap_servers=[KAFKA_NODES],
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)
'''Code for Finnhub API (realtime, using Websocket)'''

class StockProducer:
    """ 
    :param api_key:
    :param topic_name:
    """
    
    topic_name: str = ""
    seen_records = set()
    
    def __init__(self, api_key: str, topic_name: str) -> None:
        self.api_key = api_key
        StockProducer.topic_name = topic_name
        
        
    def finnhub_websocket_collect(self) -> None:
        """ 
        Establish connection to stock api
        :param stock_code: Stock code for which you want to get data
        """
        
        try:
            if self.api_key is not None:
                websocket.enableTrace(True)
                ws = websocket.WebSocketApp(
                    f"wss://ws.finnhub.io?token={self.api_key}",
                    on_message=StockProducer.on_message,
                    on_error=StockProducer.on_error,
                    on_close=StockProducer.on_close,
                    on_ping=StockProducer.on_ping,
                    on_pong=StockProducer.on_pong
                )
                ws.on_open = StockProducer.on_open
                ws.run_forever(ping_interval=30)
                
        except (
                ConnectionRefusedError,
                KeyboardInterrupt,
                SystemExit,
                Exception,
            ) as e:
            logger.exception(f"Connect failed: {str(e)}")   
            
            
    @staticmethod
    def on_message(ws, message) -> None:
        
        try:
            data = json.loads(message)
            if data.get("type") == "ping":
                logger.debug("Received ping from Finnhub")
                return

            # Handle error messages  
            if data.get("type") == "error":
                logger.error(f"Finnhub Error: {data.get('msg')}")
                return
            
            if data is not None or "data" in data:
                for record_data in data["data"]:
                    record_key = (
                        str(record_data["c"]), 
                        record_data["p"], 
                        record_data["s"],
                        record_data["t"],
                        record_data["v"],
                    )
                    if record_key not in StockProducer.seen_records:
                        StockProducer.seen_records.add(record_key)
                    
                        producer.send(
                            topic=StockProducer.topic_name,
                            value=record_data
                        )
                        # producer.flush()
                        logger.info(f"Collect success {record_data}")
                        
                    else: logger.warning(f"Duplicate record skipped: {record_data}")
                    
        except Exception as e:
            logger.error(f"Error in on_message: {str(e)}")
        
        
    @staticmethod
    def on_error(ws, error) -> None: 
        logger.error(f"WebSocket Error: {error}")
        
        
    @staticmethod
    def on_close(ws, close_status_code, close_msg) -> None:
        logger.warning(f"WebSocket closed. code={close_status_code}, msg={close_msg}")

    
    
    @staticmethod
    def on_open(ws) -> None:        
        for stock_code in stock_code_constant.STOCKCODE[:5]:
            message = f'{{"type":"subscribe","symbol":"{stock_code}"}}'
            ws.send(message)
            logger.info(f"Subscribed to {stock_code}.")
            
            
    @staticmethod
    def on_ping(ws, message):
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        logger.info("Ping sent")
        
        
    @staticmethod
    def on_pong(ws, message):
        """ 
        Establish connection to yahoo finance api
        :param stock_code: Stock code for which you want to get data
        """
        logger.info("Pong received")

                
                
                
if __name__ == "__main__":
    finnhubApi = StockProducer(
        api_key=os.getenv("FINNHUB_API_KEY"),
        topic_name="finnhub_stock"
    )
    finnhubApi.finnhub_websocket_collect()