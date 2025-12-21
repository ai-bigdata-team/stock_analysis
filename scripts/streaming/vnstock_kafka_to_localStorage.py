"""
Kafka to Cassandra Streaming Pipeline for VNSTOCK Data
Đọc real-time OHLCV data từ StockProducer (vnstock) và ghi vào Cassandra
Author: BigData Team
Date: 2025-10-29
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp,
    avg, window, count, sum as spark_sum, 
    min as spark_min, max as spark_max, last, first
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    FloatType, DoubleType, LongType, IntegerType, TimestampType
)


class VnstockKafkaCassandraStreaming:
    """
    Streaming pipeline cho VNSTOCK data: Kafka → Cassandra
    """
    
    def __init__(self, 
                 kafka_servers="localhost:9092", 
                 cassandra_host="localhost",
                 cassandra_keyspace="stock_market"):
        """
        Khởi tạo streaming pipeline
        
        Args:
            kafka_servers: Kafka bootstrap servers
            cassandra_host: Cassandra host
            cassandra_keyspace: Cassandra keyspace name
        """
        self.kafka_servers = kafka_servers
        self.cassandra_host = cassandra_host
        self.cassandra_keyspace = cassandra_keyspace
        self.spark = None
        
    def create_spark_session(self):
        """Tạo Spark Session với config cho streaming"""
        print("Creating Spark Session for VNSTOCK streaming...")
        
        self.spark = SparkSession.builder \
            .appName("Vnstock-Kafka-Streaming") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark Session created successfully")
        return self.spark
    
    def get_vnstock_schema(self):
        """
        Schema cho VNSTOCK OHLCV messages
        
        Format từ StockProducer:
        {
            "time": "2024-10-29T00:00:00.000",
            "open": 30500.0,
            "high": 30800.0,
            "low": 30400.0,
            "close": 30750.0,
            "volume": 2500000,
            "ticker": "HPG"
        }
        """
        return StructType([
            StructField("time", StringType(), True),      # ISO datetime string
            StructField("open", DoubleType(), True),      # Open price
            StructField("high", DoubleType(), True),      # High price
            StructField("low", DoubleType(), True),       # Low price
            StructField("close", DoubleType(), True),     # Close price
            StructField("volume", LongType(), True),      # Volume
            StructField("ticker", StringType(), True)     # Stock symbol
        ])
    
    def read_from_kafka(self, topic="vnstock_stock"):
        """
        Đọc streaming data từ Kafka
        
        Args:
            topic: Kafka topic name (default: vnstock_stock)
            
        Returns:
            DataFrame: Raw Kafka stream
        """
        print(f" Reading from Kafka topic: {topic}")
        
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", "5000") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print(" Kafka stream connected")
        return kafka_df
    
    def parse_vnstock_messages(self, kafka_df):
        """
        Parse VNSTOCK JSON messages từ Kafka
        
        Args:
            kafka_df: Raw Kafka DataFrame
            
        Returns:
            DataFrame: Parsed OHLCV data
        """
        print(" Parsing VNSTOCK messages...")
        
        schema = self.get_vnstock_schema()
        
        # Parse JSON từ value column
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Extract các trường
        ohlcv_df = parsed_df.select(
            col("data.ticker").alias("symbol"),
            col("data.time").alias("time_string"),
            col("data.open").alias("open"),
            col("data.high").alias("high"),
            col("data.low").alias("low"),
            col("data.close").alias("close"),
            col("data.volume").alias("volume"),
            col("kafka_timestamp")
        )
        
        # Convert time string to timestamp
        # Format: "2024-10-29T00:00:00.000" hoặc ISO 8601
        ohlcv_df = ohlcv_df.withColumn(
            "trade_timestamp",
            to_timestamp(col("time_string"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
        )
        
        # Nếu conversion thất bại, thử format khác
        ohlcv_df = ohlcv_df.withColumn(
            "trade_timestamp",
            col("trade_timestamp")
        )
        
        # Thêm các cột tính toán
        ohlcv_df = ohlcv_df \
            .withColumn("price_range", col("high") - col("low")) \
            .withColumn("price_change", col("close") - col("open")) \
            .withColumn("price_change_pct", 
                       ((col("close") - col("open")) / col("open") * 100)) \
            .withColumn("ingest_timestamp", current_timestamp())
        
        # Filter null timestamps
        ohlcv_df = ohlcv_df.filter(col("trade_timestamp").isNotNull())
        
        print(" Messages parsed successfully")
        return ohlcv_df
    
    def create_aggregates(self, ohlcv_df, window_duration="5 minutes"):
        """
        Tạo aggregates cho VNSTOCK data
        
        Args:
            ohlcv_df: DataFrame chứa OHLCV data
            window_duration: Độ dài window (e.g., "5 minutes", "1 hour")
            
        Returns:
            DataFrame: Aggregated data
        """
        print(f" Creating aggregates with {window_duration} windows...")
        
        aggregates_df = ohlcv_df \
            .withWatermark("trade_timestamp", "1 minute") \
            .groupBy(
                col("symbol"),
                window(col("trade_timestamp"), window_duration)
            ) \
            .agg(
                count("*").alias("record_count"),
                first("open").alias("window_open"),
                last("close").alias("window_close"),
                spark_max("high").alias("window_high"),
                spark_min("low").alias("window_low"),
                spark_sum("volume").alias("total_volume"),
                avg("close").alias("avg_close_price"),
                avg("volume").alias("avg_volume"),
                spark_sum(col("close") * col("volume")).alias("total_turnover")
            )
        
        # Flatten window struct và tính toán thêm
        aggregates_df = aggregates_df.select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("record_count"),
            col("window_open"),
            col("window_close"),
            col("window_high"),
            col("window_low"),
            col("total_volume"),
            col("avg_close_price"),
            col("avg_volume"),
            col("total_turnover")
        )
        
        # Thêm các chỉ số
        aggregates_df = aggregates_df \
            .withColumn("window_price_change", 
                       col("window_close") - col("window_open")) \
            .withColumn("window_price_change_pct",
                       ((col("window_close") - col("window_open")) / 
                        col("window_open") * 100)) \
            .withColumn("window_price_range",
                       col("window_high") - col("window_low")) \
            .withColumn("computed_at", current_timestamp())
        
        print(" Aggregates created")
        return aggregates_df
    
    def write_to_cassandra_raw_ohlcv(self, ohlcv_df):
        """
        Ghi raw OHLCV data vào Cassandra table: vnstock_ohlcv
        
        Args:
            ohlcv_df: DataFrame chứa OHLCV data
            
        Returns:
            StreamingQuery: Streaming query object
        """
        print(" Writing raw OHLCV to Cassandra...")
        
        # Select columns cho Cassandra
        output_df = ohlcv_df.select(
            col("symbol"),
            col("trade_timestamp"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("price_range"),
            col("price_change"),
            col("price_change_pct"),
            col("ingest_timestamp")
        )
        
        query = output_df \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(self._write_batch_to_cassandra_ohlcv) \
            .trigger(processingTime="10 seconds") \
            .option("checkpointLocation", "./checkpoints/vnstock_raw") \
            .start()
        
        print(" Raw OHLCV streaming query started")
        return query
    
    def write_to_cassandra_aggregates(self, aggregates_df):
        """
        Ghi aggregates vào Cassandra table: vnstock_aggregates
        
        Args:
            aggregates_df: DataFrame chứa aggregates
            
        Returns:
            StreamingQuery: Streaming query object
        """
        print(" Writing aggregates to Cassandra...")
        
        query = aggregates_df \
            .writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_batch_to_cassandra_aggregates) \
            .trigger(processingTime="30 seconds") \
            .option("checkpointLocation", "./checkpoints/vnstock_aggregates") \
            .start()
        
        print(" Aggregates streaming query started")
        return query
    
    def _write_batch_to_cassandra_ohlcv(self, batch_df, batch_id):
        """Helper function để ghi batch vào Cassandra table: vnstock_ohlcv"""
        if batch_df.count() > 0:
            print(f" Batch {batch_id}: Writing {batch_df.count()} OHLCV records to Cassandra")
            
            # Show sample data
            print("Sample data:")
            batch_df.select("symbol", "trade_timestamp", "close", "volume").show(5, truncate=False)
            
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="vnstock_ohlcv", keyspace=self.cassandra_keyspace) \
                .save()
            
            print(f" Batch {batch_id} written successfully")
    
    def _write_batch_to_cassandra_aggregates(self, batch_df, batch_id):
        """Helper function để ghi batch vào Cassandra table: vnstock_aggregates"""
        if batch_df.count() > 0:
            print(f" Batch {batch_id}: Writing {batch_df.count()} aggregate records to Cassandra")
            
            # Show sample data
            print("Sample aggregates:")
            batch_df.select("symbol", "window_start", "window_close", "total_volume").show(5, truncate=False)
            
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="vnstock_aggregates", keyspace=self.cassandra_keyspace) \
                .save()
            
            print(f" Batch {batch_id} written successfully")
    
    def run_pipeline(self, kafka_topic="vnstock_stock", window_duration="5 minutes"):
        """
        Chạy toàn bộ pipeline: Kafka → Transform → Cassandra
        
        Args:
            kafka_topic: Kafka topic name (default: vnstock_stock)
            window_duration: Aggregation window duration
        """
        print("\n" + "="*70)
        print(" STARTING VNSTOCK KAFKA → CASSANDRA STREAMING PIPELINE")
        print("="*70 + "\n")
        
        # 1. Create Spark session
        self.create_spark_session()
        
        # 2. Read from Kafka
        kafka_df = self.read_from_kafka(kafka_topic)
        
        # 3. Parse messages
        ohlcv_df = self.parse_vnstock_messages(kafka_df)
        
        # 4. Create aggregates
        aggregates_df = self.create_aggregates(ohlcv_df, window_duration)
        
        # 5. Write to Cassandra - Raw OHLCV
        query1 = self.write_to_cassandra_raw_ohlcv(ohlcv_df)
        
        # 6. Write to Cassandra - Aggregates
        query2 = self.write_to_cassandra_aggregates(aggregates_df)
        
        print("\n" + "="*70)
        print(" VNSTOCK PIPELINE STARTED SUCCESSFULLY")
        print("="*70)
        print("\n Monitoring:")
        print("  - Spark UI: http://localhost:4040")
        print("  - Checkpoints: ./checkpoints/vnstock_*")
        print(f"  - Kafka topic: {kafka_topic}")
        print(f"  - Window duration: {window_duration}")
        print("\n Data flow:")
        print("  StockProducer (vnstock) → Kafka → Spark → Cassandra")
        print("\n Press Ctrl+C to stop\n")
        
        # Wait for termination
        try:
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            print("\n\n Stopping streaming queries...")
            query1.stop()
            query2.stop()
            print(" Streaming stopped gracefully")


def main():
    """Main function"""
    # Cấu hình
    KAFKA_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "vnstock_stock"  # Topic từ StockProducer
    CASSANDRA_HOST = "localhost"
    CASSANDRA_KEYSPACE = "stock_market"
    WINDOW_DURATION = "5 minutes"  # Phù hợp với polling interval của vnstock
    
    print("\n" + "="*70)
    print(" VNSTOCK STREAMING CONFIGURATION")
    print("="*70)
    print(f"Kafka servers: {KAFKA_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Cassandra host: {CASSANDRA_HOST}")
    print(f"Cassandra keyspace: {CASSANDRA_KEYSPACE}")
    print(f"Aggregation window: {WINDOW_DURATION}")
    print("="*70 + "\n")
    
    # Khởi tạo và chạy pipeline
    pipeline = VnstockKafkaCassandraStreaming(
        kafka_servers=KAFKA_SERVERS,
        cassandra_host=CASSANDRA_HOST,
        cassandra_keyspace=CASSANDRA_KEYSPACE
    )
    
    pipeline.run_pipeline(
        kafka_topic=KAFKA_TOPIC,
        window_duration=WINDOW_DURATION
    )


if __name__ == "__main__":
    main()
