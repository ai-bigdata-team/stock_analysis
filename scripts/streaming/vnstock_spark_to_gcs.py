from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, sum as spark_sum,
    min as spark_min, max as spark_max, expr, to_timestamp,
    first, last, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
import argparse
from dotenv import load_dotenv
load_dotenv()

def create_spark_session(gcs_key_path):
    """
    Tạo Spark Session với GCS connector - OPTIMIZED cho high throughput
    
    Yêu cầu:
    - GCS connector JAR: gcs-connector-hadoop3-latest.jar
    - Service Account JSON key file
    """
    return SparkSession.builder \
        .appName("VNStockKafkaToGCS") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key_path) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "6") \
        .config("spark.default.parallelism", "6") \
        .config("spark.sql.streaming.fileSource.log.compactInterval", "1000") \
        .config("spark.hadoop.fs.gs.outputstream.buffer.size", "33554432") \
        .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "33554432") \
        .getOrCreate()

def get_vnstock_schema():
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

def process_raw_ohlcv(df, checkpoint_dir, output_path):
    """
    Xử lý raw OHLCV và lưu vào GCS
    
    Output path format: gs://your-bucket/datalake/raw/ohlcv/symbol=HPG/
    """
    # Parse và transform data
    ohlcv_df = df \
        .select(
            col("ticker").alias("symbol"),
            to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("trade_timestamp"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            (col("high") - col("low")).alias("price_range"),
            (col("close") - col("open")).alias("price_change"),
            ((col("close") - col("open")) / col("open") * 100).alias("price_change_pct"),
            expr("uuid()").alias("record_id"),
            current_timestamp().alias("ingest_timestamp")
        ) \
        .filter(col("trade_timestamp").isNotNull()) \
        .withWatermark("trade_timestamp", "2 minutes")
    
    # Lưu vào GCS với partitioning theo symbol
    query = ohlcv_df \
        .writeStream \
        .queryName("VNStock_Raw_OHLCV_to_GCS") \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/raw_ohlcv") \
        .option("path", f"{output_path}/raw/ohlcv") \
        .partitionBy("symbol") \
        .trigger(processingTime="30 seconds") \
        .option("maxRecordsPerFile", "50000") \
        .start()
    
    return query, ohlcv_df

def process_aggregates(ohlcv_df, checkpoint_dir, output_path, window_duration="5 minutes"):
    """
    Tính aggregates theo window và lưu vào GCS
    
    Output: gs://your-bucket/datalake/aggregates/ohlcv/symbol=HPG/
    """
    aggregates_df = ohlcv_df \
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
            avg("volume").alias("avg_volume"),
            spark_sum(col("close") * col("volume")).alias("total_turnover")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("record_count"),
            col("window_open"),
            col("window_close"),
            col("window_high"),
            col("window_low"),
            col("total_volume"),
            col("avg_close_price"),
            col("avg_volume"),
            col("total_turnover"),
            ((col("window_close") - col("window_open")) / col("window_open") * 100).alias("window_price_change_pct"),
            (col("window_high") - col("window_low")).alias("window_price_range"),
            current_timestamp().alias("computed_at")
        )
    
    query = aggregates_df \
        .writeStream \
        .queryName("VNStock_Aggregates_to_GCS") \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/aggregates") \
        .option("path", f"{output_path}/aggregates/ohlcv") \
        .partitionBy("symbol") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query

def get_args():
    parser = argparse.ArgumentParser(description="VNStock Kafka to GCS streaming pipeline")
    parser.add_argument("--bucket-name", default="stock_data_demo_vnstock", 
                        help="GCS bucket name (e.g., my-stock-datalake)")
    parser.add_argument("--gcs-key", default=os.environ["GCS_JSON_KEY"], 
                        help="Path to GCS service account JSON key file")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="vnstock_stock",
                        help="Kafka topic name")
    parser.add_argument("--window-duration", default="1 minutes",
                        help="Aggregation window duration (e.g., '5 minutes', '1 hour')")
    args = parser.parse_args()
    return args

def main():
    """
    Main function
    
    GCS Path Structure:
    gs://your-datalake-bucket/
    ├── raw/
    │   └── ohlcv/
    │       └── symbol=HPG/
    │           └── part-00000-xxx.parquet
    ├── aggregates/
    │   └── ohlcv/
    │       └── symbol=HPG/
    │           └── part-00000-xxx.parquet
    └── checkpoints/
        ├── raw_ohlcv/
        └── aggregates/
    """
    args = get_args()
    
    # Expand ~ trong path
    gcs_key_path = os.path.expanduser(args.gcs_key)
    
    # Validate key file exists
    if not os.path.exists(gcs_key_path):
        raise FileNotFoundError(f"GCS key file not found: {gcs_key_path}")
    
    # Cấu hình GCS paths
    GCS_BUCKET = args.bucket_name  
    GCS_BASE_PATH = f"gs://{GCS_BUCKET}/vnstock_data"
    GCS_CHECKPOINT = f"gs://{GCS_BUCKET}/checkpoints/vnstock"
    
    spark = create_spark_session(gcs_key_path)
    
    # Sync shuffle partitions with actual available cores
    total_cores = spark.sparkContext.defaultParallelism
    spark.conf.set("spark.sql.shuffle.partitions", str(total_cores))
    spark.conf.set("spark.default.parallelism", str(total_cores))
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Tắt WARNING cho HDFSBackedStateStoreProvider
    log4j = spark._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider").setLevel(log4j.Level.ERROR)
    
    print(f"""
    ================================
    VNStock Configuration
    ================================
    Total cores: {total_cores}
    Shuffle partitions: {total_cores}
    GCS Bucket: {GCS_BUCKET}
    Kafka servers: {args.kafka_servers}
    Kafka topic: {args.kafka_topic}
    Window duration: {args.window_duration}
    ================================
    """)
    
    # Đọc từ Kafka với optimized settings
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_servers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "1000") \
        .option("kafka.fetch.max.bytes", "52428800") \
        .option("kafka.max.partition.fetch.bytes", "10485760") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON
    schema = get_vnstock_schema()
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("parsed_data")
    ).select("parsed_data.*")
    
    # Xử lý raw OHLCV
    raw_query, ohlcv_df = process_raw_ohlcv(
        parsed_df, 
        GCS_CHECKPOINT,
        GCS_BASE_PATH
    )
    
    # Xử lý aggregates
    agg_query = process_aggregates(
        ohlcv_df,
        GCS_CHECKPOINT,
        GCS_BASE_PATH,
        args.window_duration
    )
    
    print(f"""
    ================================
    VNStock → GCS DataLake Pipeline
    ================================
    Kafka Topic: {args.kafka_topic}
    GCS Output: {GCS_BASE_PATH}
    Checkpoint: {GCS_CHECKPOINT}
    
    Raw OHLCV: {GCS_BASE_PATH}/raw/ohlcv/
    Aggregates: {GCS_BASE_PATH}/aggregates/ohlcv/
    
    Streaming queries started...
    Press Ctrl+C to stop
    ================================
    """)
    
    # Chờ cả 2 queries
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n\nStopping streaming queries...")
        raw_query.stop()
        agg_query.stop()
        print("Streaming stopped gracefully")

if __name__ == "__main__":
    main()
