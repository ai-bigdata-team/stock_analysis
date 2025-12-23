import os
import json
import time
import logging
import argparse
import pandas as pd
from datetime import datetime
from vnstock import Vnstock
from kafka.producer import KafkaProducer
from .stock_code_constant import STOCKCODE
from typing import Union, Any
from dotenv import load_dotenv
load_dotenv()

'''Collect real time stock data and send to Kafka topic'''


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



KAFKA_NODES = os.getenv("KAFKA_NODES")
producer = KafkaProducer(bootstrap_servers=[KAFKA_NODES],
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)


class StockProducer:
    """
    :param tickerSymbols
    """
    
    def __init__(self, nameSource: str = "Vnstock API", source: str = "VCI") -> None:
        self.nameSource: str = nameSource
        self.source: str = source  # VCI, TCBS, MSN
        
        
    def vnstock_connect(self, stock_code: str):
        """ 
        Establish connection to vnstock api
        :param stock_code: Stock code for which you want to get data
        """
        
        try:
            if STOCKCODE is not None:
                if stock_code in STOCKCODE:
                    logger.info(f"Connect success to {self.nameSource}.")
                    return Vnstock().stock(symbol=stock_code, source=self.source)
                else: 
                    logger.info(f"Connect failed to {self.nameSource}. Stock code {stock_code} not in list.")
                    return None
        except Exception as e: 
            logger.error(f"Error connecting to vnstock: {e}")
            raise e
        
        
    def get_OHLCV_data_realtime(
        self, stock_code: str, start_date: str = None, end_date: str = None
    ) -> dict[str, Union[str, float]]:
        """ 
        Basic and important indicators when analyzing the price of a stock 
        or asset, including: Open, High, Low, Close, Volume,...
        
        :param stock_code: Stock code for which you want to get data
        :param start_date: Start date in format 'YYYY-MM-DD' (e.g., '2024-01-01')
        :param end_date: End date in format 'YYYY-MM-DD' (e.g., '2024-12-31')
        
        Note: vnstock doesn't support real-time 1-minute interval like yfinance.
        It provides historical data with daily frequency.
        """
        
        try:
            if stock_code not in STOCKCODE:
                logger.info(f"{stock_code} is not exists.")
                return None
                
            vnstockConnect = self.vnstock_connect(stock_code)
            if vnstockConnect is None:
                return None
                
            logger.info(f"Connect success to {self.nameSource}")
            
            # Set default dates if not provided
            if end_date is None:
                end_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            if start_date is None:
                # Get data for last 5 days
                start_date = (pd.Timestamp.now() - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
            
            # Get historical data from vnstock
            getStockInformationDF = vnstockConnect.quote.history(
                symbol=stock_code,
                start=start_date,
                end=end_date
            )
            
            if getStockInformationDF is None or getStockInformationDF.empty:
                logger.warning(f"No data available for {stock_code}")
                return None
            
            getStockInformationDF = getStockInformationDF.reset_index()
            getStockInformationDF["ticker"] = stock_code
            pd.set_option('display.max_rows', None)
            
            # Get the latest record
            records_latest: pd.DataFrame = getStockInformationDF.tail(1)
            if records_latest is not None and not records_latest.empty:
                print("Data")
                print(records_latest)
                record_data = records_latest.to_json(
                    date_format='iso',
                    orient="records"            
                )
                time.sleep(0.5)
                return json.loads(str(record_data)[1:-1])
            else: 
                logger.warning(f"No records available for {stock_code}.")
                return None

        except Exception as e: 
            logger.error(f"Error getting OHLCV data: {e}")
            raise e
    
    
    def get_OHLCVs_data_realtime(
        self, topic: str, start_date: str = None, end_date: str = None, polling_interval: int = 60
    ) -> None:
        """ 
        Basic and important indicators when analyzing stock or asset prices, 
        including: Open, High, Low, Close, Volume,.... 
        Taken with many stock codes.
        
        :param topic: Kafka topic to send data
        :param start_date: Start date in format 'YYYY-MM-DD'
        :param end_date: End date in format 'YYYY-MM-DD'
        :param polling_interval: Time in seconds between data polls (default 60s)
        
        Note: vnstock provides historical data, not real-time streaming.
        This will poll the API at specified intervals.
        """
        
        try:
            while True:
                # Stop if current time is past 09:00 UTC (16:00 VN)
                if datetime.now().hour >= 20:
                    logger.info("Reached 09:00 UTC. Stopping the producer.")
                    break

                for stock_code in STOCKCODE:
                    stock_json_record = self.get_OHLCV_data_realtime(
                        stock_code=stock_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    if stock_json_record is not None:
                        print(stock_json_record)
                        producer.send(topic=topic, value=stock_json_record)
                        time.sleep(0.5)
                    else:
                        logger.warning(f"No data for {stock_code}, skipping...")
                    
                # Wait before next poll (vnstock doesn't support real-time streaming)
                logger.info(f"Waiting {polling_interval} seconds before next poll...")
                time.sleep(polling_interval)
                
        except Exception as e: 
            logger.error(f"Error in get_OHLCVs_data_realtime: {e}")
            raise e
            
            
    def get_stock_trade_realtime(self) -> Any:
        pass
        
        
        
if __name__ == "__main__":
    stockProducer = StockProducer(nameSource="Vnstock API", source="VCI")
    # Test with a single stock
    print(stockProducer.get_OHLCV_data_realtime("HPG"))
    # Start collecting data for all stocks and send to Kafka
    stockProducer.get_OHLCVs_data_realtime("vnstock_stock")