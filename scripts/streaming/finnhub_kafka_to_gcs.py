"""
Finnhub Kafka to Google Cloud Storage (GCS) DataLake Pipeline
Đọc từ Kafka topic 'finnhub_stock', xử lý và lưu vào GCS
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, sum as spark_sum,
    min as spark_min, max as spark_max, expr, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
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
        .appName("FinnhubKafkaToGCS") \
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
        .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
        .config("spark.sql.streaming.fileSource.log.compactInterval", "1000") \
        .config("spark.hadoop.fs.gs.outputstream.buffer.size", "33554432") \
        .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "33554432") \
        .getOrCreate()

def get_finnhub_schema():
    """Schema cho Finnhub trade data - single trade per message"""
    return StructType([
        StructField("c", ArrayType(StringType()), True),  # Trade conditions
        StructField("p", DoubleType(), True),              # Price
        StructField("s", StringType(), True),              # Symbol
        StructField("t", LongType(), True),                # Timestamp (milliseconds)
        StructField("v", DoubleType(), True)               # Volume
    ])

def process_raw_trades(df, checkpoint_dir, output_path):

    """
    Xử lý raw trades và lưu vào GCS - OPTIMIZED
    
    Output path format: gs://your-bucket/datalake/raw/trades/symbol=AAPL/
    """
    # Data already in flat structure, no need to explode
    trades_df = df \
        .select(
            col("s").alias("symbol"),
            col("p").alias("price"),
            col("v").alias("volume"),
            to_timestamp(col("t") / 1000).alias("trade_timestamp"),
            col("c").alias("conditions"),
            expr("uuid()").alias("trade_id")
        ) \
        .withWatermark("trade_timestamp", "2 minutes") \
        .repartition(4, "symbol") 
    
    # Lưu vào GCS với trigger nhanh hơn
    query = trades_df \
        .writeStream \
        .queryName("Raw_Trades_to_GCS") \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/raw_trades") \
        .option("path", f"{output_path}/raw/trades") \
        .partitionBy("symbol") \
        .trigger(processingTime="10 seconds") \
        .option("maxRecordsPerFile", "50000") \
        .start()
    
    return query, trades_df

def process_aggregates(trades_df, checkpoint_dir, output_path):

    """
    Tính aggregates theo 1-minute windows và lưu vào GCS - OPTIMIZED
    
    Output: gs://your-bucket/datalake/aggregates/ohlcv/symbol=AAPL/
    """
    aggregates_df = trades_df \
        .groupBy(
            window(col("trade_timestamp"), "1 minute"),  # Window nhỏ hơn
            col("symbol")
        ) \
        .agg(
            expr("first(price)").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            expr("last(price)").alias("close"),
            spark_sum("volume").alias("total_volume"),
            count("*").alias("trade_count"),
            avg("price").alias("avg_price")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("total_volume"),
            col("trade_count"),
            col("avg_price")
        )
    
    query = aggregates_df \
        .writeStream \
        .queryName("Aggregates_1min_to_GCS") \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/aggregates") \
        .option("path", f"{output_path}/aggregates/ohlcv") \
        .partitionBy("symbol") \
        .trigger(processingTime="30 seconds") \
        .option("maxRecordsPerFile", "5000") \
        .start()
    
    return query

def get_args():
    parser = argparse.ArgumentParser(description="Code to load data from Kafka to GCS")
    parser.add_argument("--bucket-name", default="stock_data_demo", 
                        help="GCS bucket name (e.g., my-stock-datalake)")
    parser.add_argument("--gcs-key", default=os.environ["GCS_JSON_KEY"], 
                        help="Path to GCS service account JSON key file")
    args = parser.parse_args()
    return args

def main():
    
    """
    Main function
    
    GCS Path Structure:
    gs://your-datalake-bucket/
    ├── raw/
    │   └── trades/
    │       └── symbol=AAPL/
    │           └── part-00000-xxx.parquet
    ├── aggregates/
    │   └── ohlcv/
    │       └── symbol=AAPL/
    │           └── part-00000-xxx.parquet
    └── checkpoints/
        ├── raw_trades/
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
    GCS_BASE_PATH = f"gs://{GCS_BUCKET}/stock_data"
    GCS_CHECKPOINT = f"gs://{GCS_BUCKET}/checkpoints"

    
    spark = create_spark_session(gcs_key_path)
    total_cores = spark.sparkContext.defaultParallelism
    spark.conf.set("spark.sql.shuffle.partitions", str(total_cores))
    spark.conf.set("spark.default.parallelism", str(total_cores))

    spark.sparkContext.setLogLevel("WARN")
    
    # Đọc từ Kafka với optimized settings
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "finnhub_stock") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()
    
    # Parse JSON
    schema = get_finnhub_schema()
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("parsed_data")
    ).select("parsed_data.*")
    
    # Xử lý raw trades
    raw_query, trades_df = process_raw_trades(
        parsed_df, 
        GCS_CHECKPOINT,
        GCS_BASE_PATH
    )
    
    # Xử lý aggregates
    agg_query = process_aggregates(
        trades_df,
        GCS_CHECKPOINT,
        GCS_BASE_PATH
    )
    
    print(f"""
    ================================
    Finnhub → GCS DataLake Pipeline
    ================================
    Kafka Topic: finnhub_stock
    GCS Output: {GCS_BASE_PATH}
    Checkpoint: {GCS_CHECKPOINT}
    
    Raw trades: {GCS_BASE_PATH}/raw/trades/
    Aggregates: {GCS_BASE_PATH}/aggregates/ohlcv/
    
    Streaming queries started...
    """)
    
    # Chờ cả 2 queries
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
