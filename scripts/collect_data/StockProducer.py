import os
import json
import time
import logging
import argparse
import pandas as pd
from vnstock import Vnstock
from kafka.producer import KafkaProducer
from ..constant import stock_code_constant
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
    
    def __init__(self, nameSource: str = "Vnstock API", source: str = "VCI", interval: str = "1h") -> None:
        self.nameSource: str = nameSource
        self.source: str = source  # VCI, TCBS, MSN
        self.interval: str = interval  # 1m, 5m, 15m, 30m, 1h, 1d
        
        
    def vnstock_connect(self, stock_code: str):
        """ 
        Establish connection to vnstock api
        :param stock_code: Stock code for which you want to get data
        """
        
        try:
            if stock_code_constant.STOCKCODE is not None:
                if stock_code in stock_code_constant.STOCKCODE:
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
    ) -> list[dict[str, Union[str, float]]]:
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
            if stock_code not in stock_code_constant.STOCKCODE:
                logger.info(f"{stock_code} is not exists.")
                return None
                
            vnstockConnect = self.vnstock_connect(stock_code)
            if vnstockConnect is None:
                return None
                
            logger.info(f"Connect success to {self.nameSource}")
            
            # Get data for last 5 days with hourly interval
            now = pd.Timestamp.now()
            
            if end_date is None:
                # If weekend (Saturday=5, Sunday=6), use Friday as end date
                current_date = now
                if now.weekday() == 5:  # Saturday
                    current_date = now - pd.Timedelta(days=1)
                    logger.info(f"🗓️  Today is Saturday, using Friday as end date")
                elif now.weekday() == 6:  # Sunday
                    current_date = now - pd.Timedelta(days=2)
                    logger.info(f"🗓️  Today is Sunday, using Friday as end date")
                
                end_date = current_date.strftime('%Y-%m-%d')
                
            if start_date is None:
                # Get data for last 5 days
                start_date = (now - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
            
            logger.info(f"📅 Fetching data for {stock_code}")
            logger.info(f"   Period: {start_date} to {end_date} (5 days)")
            logger.info(f"   Interval: {self.interval} (hourly)")
            logger.info(f"   Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} ({now.strftime('%A')})")
            
            # Get historical data from vnstock
            getStockInformationDF = vnstockConnect.quote.history(
                symbol=stock_code,
                start=start_date,
                end=end_date,
                interval=self.interval
            )
            
            if getStockInformationDF is None or getStockInformationDF.empty:
                logger.warning(f"No data available for {stock_code}")
                return None
            
            getStockInformationDF = getStockInformationDF.reset_index()
            getStockInformationDF["ticker"] = stock_code
            pd.set_option('display.max_rows', None)
            
            logger.info(f"📊 Retrieved {len(getStockInformationDF)} records for {stock_code}")
            
            # Return all records
            if getStockInformationDF is not None and not getStockInformationDF.empty:
                logger.info(f"\n{'='*60}")
                logger.info(f"📈 All data for {stock_code}:")
                logger.info(f"\n{getStockInformationDF.to_string()}")
                logger.info(f"{'='*60}\n")
                record_data = getStockInformationDF.to_json(
                    date_format='iso',
                    orient="records"            
                )
                time.sleep(0.5)
                return json.loads(record_data)
            else: 
                logger.warning(f"No records available for {stock_code}.")
                return None

        except Exception as e: 
            logger.error(f"Error getting OHLCV data: {e}")
            raise e
    
    
    def get_OHLCVs_data_realtime(
        self, topic: str, start_date: str = None, end_date: str = None, polling_interval: int = 3600
    ) -> None:
        """ 
        Basic and important indicators when analyzing stock or asset prices, 
        including: Open, High, Low, Close, Volume,.... 
        Taken with many stock codes.
        
        :param topic: Kafka topic to send data
        :param start_date: Start date in format 'YYYY-MM-DD'
        :param end_date: End date in format 'YYYY-MM-DD'
        :param polling_interval: Time in seconds between data polls (default 3600s = 1 hour)
        
        Note: vnstock provides historical data, not real-time streaming.
        This will poll the API at specified intervals.
        """
        
        try:
            cycle_count = 0
            while True:
                cycle_count += 1
                logger.info(f"\n{'='*80}")
                logger.info(f"🔄 Starting data collection cycle #{cycle_count}")
                logger.info(f"⏰ Time: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*80}\n")
                
                successful_sends = 0
                failed_stocks = []
                total_records_sent = 0
                
                for stock_code in stock_code_constant.STOCKCODE:
                    stock_json_records = self.get_OHLCV_data_realtime(
                        stock_code=stock_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    if stock_json_records is not None:
                        logger.info(f"📤 Sending {len(stock_json_records)} records for {stock_code} to Kafka topic: {topic}")
                        for record in stock_json_records:
                            logger.info(f"   Record: {json.dumps(record, indent=2)}")
                            producer.send(topic=topic, value=record)
                            total_records_sent += 1
                            time.sleep(0.1)
                        successful_sends += 1
                        time.sleep(0.5)
                    else:
                        logger.warning(f"⚠️  No data for {stock_code}, skipping...")
                        failed_stocks.append(stock_code)
                
                logger.info(f"\n{'='*80}")
                logger.info(f"✅ Cycle #{cycle_count} completed")
                logger.info(f"   Successfully sent: {successful_sends}/{len(stock_code_constant.STOCKCODE)} stocks")
                logger.info(f"   Total records sent: {total_records_sent}")
                if failed_stocks:
                    logger.info(f"   Failed stocks: {', '.join(failed_stocks)}")
                logger.info(f"⏰ Next poll in {polling_interval} seconds ({polling_interval/3600:.1f} hours)")
                logger.info(f"   Next run at: {(pd.Timestamp.now() + pd.Timedelta(seconds=polling_interval)).strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*80}\n")
                
                time.sleep(polling_interval)
                
        except Exception as e: 
            logger.error(f"Error in get_OHLCVs_data_realtime: {e}")
            raise e
            
            
    def get_stock_trade_realtime(self) -> Any:
        pass
        
        
        
if __name__ == "__main__":
    stockProducer = StockProducer(nameSource="Vnstock API", source="VCI", interval="1h")
    # Test with a single stock
    print(stockProducer.get_OHLCV_data_realtime("HPG"))
    # Start collecting data for all stocks and send to Kafka
    stockProducer.get_OHLCVs_data_realtime("vnstock_stock")