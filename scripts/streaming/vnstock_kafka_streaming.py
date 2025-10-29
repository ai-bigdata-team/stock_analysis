from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp,
    avg, window, count, sum as spark_sum, 
    min as spark_min, max as spark_max, first, last
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, LongType, TimestampType
)


class VnstockKafkaStreaming:
    """
    Streaming pipeline cho VNSTOCK data: Kafka → Parquet
    """
    
    def __init__(self, kafka_servers="localhost:9092"):
        """
        Khởi tạo streaming pipeline
        
        Args:
            kafka_servers: Kafka bootstrap servers
        """
        self.kafka_servers = kafka_servers
        self.spark = None
        
    def create_spark_session(self):
        """Tạo Spark Session"""
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
            StructField("time", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("ticker", StringType(), True)
        ])
    
    def read_from_kafka(self, topic):
        """
        Đọc streaming data từ Kafka topic
        
        Args:
            topic: Kafka topic name
            
        Returns:
            DataFrame: Streaming DataFrame
        """
        print(f"Reading from Kafka topic: {topic}")
        
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", "5000") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("Kafka stream connected")
        return kafka_df
    
    def parse_vnstock_data(self, kafka_df):
        """
        Parse JSON data từ Kafka và tính toán các metrics
        
        Args:
            kafka_df: Raw Kafka DataFrame
            
        Returns:
            DataFrame: Parsed DataFrame với OHLCV data
        """
        print("Parsing VNSTOCK data...")
        
        schema = self.get_vnstock_schema()
        
        # Parse JSON
        parsed_df = kafka_df \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), schema).alias("data")) \
            .select("data.*")
        
        # Convert timestamp và tính toán metrics
        ohlcv_df = parsed_df \
            .withColumn(
                "trade_timestamp",
                to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
            ) \
            .withColumn("ingest_timestamp", current_timestamp()) \
            .withColumn("symbol", col("ticker")) \
            .withColumn("price_range", col("high") - col("low")) \
            .withColumn("price_change", col("close") - col("open")) \
            .withColumn(
                "price_change_pct",
                ((col("close") - col("open")) / col("open")) * 100
            ) \
            .drop("time", "ticker")
        
        print("Data parsed successfully")
        return ohlcv_df
    
    def create_aggregates(self, ohlcv_df, window_duration="5 minutes"):
        """
        Tạo windowed aggregates từ OHLCV data
        
        Args:
            ohlcv_df: DataFrame với OHLCV data
            window_duration: Window size (e.g., "5 minutes", "1 hour")
            
        Returns:
            DataFrame: Aggregated DataFrame
        """
        print(f"Creating {window_duration} aggregates...")
        
        aggregates_df = ohlcv_df \
            .withWatermark("trade_timestamp", "10 minutes") \
            .groupBy(
                window(col("trade_timestamp"), window_duration),
                col("symbol")
            ) \
            .agg(
                count("*").alias("record_count"),
                first("open").alias("window_open"),
                last("close").alias("window_close"),
                spark_max("high").alias("window_high"),
                spark_min("low").alias("window_low"),
                spark_sum("volume").alias("total_volume"),
                avg("close").alias("avg_close_price"),
                avg("volume").alias("avg_volume")
            ) \
            .select(
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
                ((col("window_close") - col("window_open")) * col("total_volume")).alias("total_turnover"),
                (col("window_close") - col("window_open")).alias("window_price_change"),
                (((col("window_close") - col("window_open")) / col("window_open")) * 100).alias("window_price_change_pct"),
                (col("window_high") - col("window_low")).alias("window_price_range")
            ) \
            .withColumn("computed_at", current_timestamp())
        
        print("Aggregates created")
        return aggregates_df
    
    def write_to_parquet(self, df, output_path, checkpoint_path, partition_by="symbol"):
        """
        Ghi streaming data vào Parquet files
        
        Args:
            df: DataFrame to write
            output_path: Output directory path
            checkpoint_path: Checkpoint directory path
            partition_by: Column to partition by
            
        Returns:
            StreamingQuery: Streaming query object
        """
        print(f"Writing to Parquet: {output_path}")
        
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy(partition_by) \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print(f"Streaming query started: {query.name or 'unnamed'}")
        return query
    
    def write_aggregates_to_parquet(self, df, output_path, checkpoint_path):
        """
        Ghi windowed aggregates vào Parquet (update mode)
        
        Args:
            df: Aggregates DataFrame
            output_path: Output directory path
            checkpoint_path: Checkpoint directory path
            
        Returns:
            StreamingQuery: Streaming query object
        """
        print(f"📊 Writing aggregates to Parquet: {output_path}")
        
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("symbol") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        print(f"Aggregates streaming query started: {query.name or 'unnamed'}")
        return query
    
    def run_pipeline(self, kafka_topic="vnstock_stock"):
        """
        Chạy toàn bộ streaming pipeline
        
        Args:
            kafka_topic: Kafka topic to consume from
        """
        print("\n" + "="*60)
        print("Starting VNSTOCK Kafka Streaming Pipeline")
        print("="*60 + "\n")
        
        # Tạo Spark session
        self.create_spark_session()
        
        # Đọc từ Kafka
        kafka_df = self.read_from_kafka(kafka_topic)
        
        # Parse data
        ohlcv_df = self.parse_vnstock_data(kafka_df)
        
        # Tạo aggregates
        aggregates_df = self.create_aggregates(ohlcv_df, window_duration="5 minutes")
        
        # Ghi raw OHLCV vào Parquet
        query1 = self.write_to_parquet(
            ohlcv_df,
            output_path="./output/vnstock_ohlcv",
            checkpoint_path="./checkpoints/vnstock_raw"
        )
        
        # Ghi aggregates vào Parquet
        query2 = self.write_aggregates_to_parquet(
            aggregates_df,
            output_path="./output/vnstock_aggregates",
            checkpoint_path="./checkpoints/vnstock_aggregates"
        )
        
        print("\n" + "="*60)
        print("Pipeline is running...")
        print("Spark UI: http://localhost:4040")
        print("Output: ./output/vnstock_ohlcv and ./output/vnstock_aggregates")
        print("Press Ctrl+C to stop")
        print("="*60 + "\n")
        
        # Wait for termination
        query1.awaitTermination()


def main():
    """Main entry point"""
    import sys
    
    kafka_servers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    kafka_topic = sys.argv[2] if len(sys.argv) > 2 else "vnstock_stock"
    
    pipeline = VnstockKafkaStreaming(kafka_servers=kafka_servers)
    
    try:
        pipeline.run_pipeline(kafka_topic=kafka_topic)
    except KeyboardInterrupt:
        print("\nStopping pipeline...")
        if pipeline.spark:
            pipeline.spark.stop()
        print("Pipeline stopped")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if pipeline.spark:
            pipeline.spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
