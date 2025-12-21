"""
VNStock Kafka to BigQuery Streaming Pipeline
===========================================
Đọc từ Kafka và ghi trực tiếp vào BigQuery (real-time streaming)

Data Format:
{"symbol":"AAA","time":1694423700000,"open":10.61,"high":10.61,"low":10.61,"close":10.61,"volume":86800}

Requirements:
- spark-sql-kafka
- spark-bigquery-connector JAR
- GCS service account key
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, sum as spark_sum,
    min as spark_min, max as spark_max, expr,
    first, last, current_timestamp, to_date, year, month
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
import argparse
from dotenv import load_dotenv

load_dotenv()

gcs_jar_path = "/mnt/d/Project/stock_analysis/jars/gcs/gcs-connector-shaded.jar"
bigquery_jar_path = "/mnt/d/Project/stock_analysis/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar"
gcs_key_path = os.path.expanduser(os.environ["GCS_JSON_KEY"])
def create_spark_session(gcs_key_path, project_id):
    """
    Tạo Spark Session với Kafka, GCS, và BigQuery connectors
    """
    return SparkSession.builder \
        .master("spark://localhost:7077") \
        .appName("VNStockKafkaToBigQuery") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.jars", gcs_jar_path + "," + bigquery_jar_path) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key_path) \
        .config("spark.hadoop.fs.gs.project.id", project_id) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "6") \
        .config("spark.default.parallelism", "6") \
        .getOrCreate()

def get_vnstock_schema():
    """
    Schema cho VNSTOCK OHLCV messages
    
    ACTUAL Format:
    {"symbol":"AAA","time":1694423700000,"open":10.61,"high":10.61,"low":10.61,"close":10.61,"volume":86800}
    """
    return StructType([
        StructField("symbol", StringType(), True),    # Stock symbol
        StructField("time", LongType(), True),        # Unix timestamp in milliseconds
        StructField("open", DoubleType(), True),      # Open price
        StructField("high", DoubleType(), True),      # High price
        StructField("low", DoubleType(), True),       # Low price
        StructField("close", DoubleType(), True),     # Close price
        StructField("volume", LongType(), True)       # Volume
    ])

def write_to_bigquery_foreachbatch(batch_df, batch_id, project_id, dataset_id, table_name, temp_bucket):
    """
    ForeachBatch function để ghi streaming data vào BigQuery
    
    Được gọi cho mỗi micro-batch trong streaming query
    """
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: No data to write")
        return
    
    print(f"Batch {batch_id}: Writing {batch_df.count()} records to BigQuery table {table_name}")
    
    try:
        target_table = f"{project_id}.{dataset_id}.{table_name}"
        
        batch_df.write \
            .format("bigquery") \
            .option("table", target_table) \
            .option("project", project_id) \
            .option("temporaryGcsBucket", temp_bucket) \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        
        print(f"Batch {batch_id}: ✅ Successfully written to {target_table}")
    except Exception as e:
        print(f"Batch {batch_id}: ❌ Error writing to BigQuery: {str(e)}")
        raise

def process_raw_ohlcv_to_bigquery(df, checkpoint_dir, project_id, dataset_id, table_name, temp_bucket):
    """
    Xử lý raw OHLCV và stream vào BigQuery
    """
    # Parse và transform data
    # Convert Unix timestamp (milliseconds) to Timestamp
    ohlcv_df = df \
        .select(
            col("symbol"),
            (col("time") / 1000).cast("timestamp").alias("trade_timestamp"),
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
        .withColumn("trade_date", to_date(col("trade_timestamp"))) \
        .withColumn("year", year(col("trade_timestamp"))) \
        .withColumn("month", month(col("trade_timestamp"))) \
        .withWatermark("trade_timestamp", "2 minutes")
    
    # Stream vào BigQuery sử dụng foreachBatch
    query = ohlcv_df \
        .writeStream \
        .queryName("VNStock_Raw_OHLCV_to_BigQuery") \
        .foreachBatch(lambda batch_df, batch_id: write_to_bigquery_foreachbatch(
            batch_df, batch_id, project_id, dataset_id, table_name, temp_bucket
        )) \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/raw_ohlcv_bigquery") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query, ohlcv_df

def process_aggregates_to_bigquery(ohlcv_df, checkpoint_dir, project_id, dataset_id, 
                                   table_name, temp_bucket, window_duration="5 minutes"):
    """
    Tính aggregates theo window và stream vào BigQuery
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
        ) \
        .withColumn("window_date", to_date(col("window_start"))) \
        .withColumn("year", year(col("window_start"))) \
        .withColumn("month", month(col("window_start")))
    
    # Stream vào BigQuery
    query = aggregates_df \
        .writeStream \
        .queryName("VNStock_Aggregates_to_BigQuery") \
        .foreachBatch(lambda batch_df, batch_id: write_to_bigquery_foreachbatch(
            batch_df, batch_id, project_id, dataset_id, table_name, temp_bucket
        )) \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/aggregates_bigquery") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query

def get_args():
    parser = argparse.ArgumentParser(description="VNStock Kafka to BigQuery streaming pipeline")
    parser.add_argument("--project-id", required=True,
                        help="GCP Project ID")
    parser.add_argument("--dataset-id", default="vnstock_data",
                        help="BigQuery dataset name")
    parser.add_argument("--bucket-name", default="stock_data_hehehe", 
                        help="GCS bucket name for temporary BigQuery writes")
    parser.add_argument("--gcs-key", default=os.environ.get("GCS_JSON_KEY"), 
                        help="Path to GCS service account JSON key file")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="vnstock_stock",
                        help="Kafka topic name")
    parser.add_argument("--window-duration", default="1 minutes",
                        help="Aggregation window duration")
    parser.add_argument("--table-prefix", default="vnstock",
                        help="Prefix for BigQuery table names")
    return parser.parse_args()

def main():
    """
    Main streaming pipeline: Kafka → BigQuery
    
    Usage:
        python vnstock_kafka_to_bigquery.py \
            --project-id my-gcp-project \
            --dataset-id vnstock_data \
            --bucket-name my-temp-bucket
    
    BigQuery Tables Created:
    - {dataset_id}.{table_prefix}_raw_ohlcv
    - {dataset_id}.{table_prefix}_aggregates_ohlcv
    """
    args = get_args()
    
    # Checkpoint location on GCS
    GCS_CHECKPOINT = f"gs://stock_data_hehehe/checkpoints/vnstock_bigquery"
    
    spark = create_spark_session(gcs_key_path, args.project_id)
    
    # Sync shuffle partitions
    total_cores = spark.sparkContext.defaultParallelism
    spark.conf.set("spark.sql.shuffle.partitions", str(total_cores))
    spark.sparkContext.setLogLevel("WARN")
    
    # print(f"""
    # ================================
    # VNStock → BigQuery Pipeline
    # ================================
    # GCP Project: {args.project_id}
    # BigQuery Dataset: {args.dataset_id}
    # Temp GCS Bucket: {args.bucket_name}
    # Kafka: {args.kafka_servers} → {args.kafka_topic}
    # Window: {args.window_duration}
    
    # Tables:
    # - {args.dataset_id}.{args.table_prefix}_raw_ohlcv
    # - {args.dataset_id}.{args.table_prefix}_aggregates_ohlcv
    # ================================
    # """)
    
    # Đọc từ Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_servers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "1000") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON
    schema = get_vnstock_schema()
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("parsed_data")
    ).select("parsed_data.*")
    
    # Stream raw OHLCV vào BigQuery
    raw_query, ohlcv_df = process_raw_ohlcv_to_bigquery(
        df=parsed_df,
        checkpoint_dir=GCS_CHECKPOINT,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_name=f"{args.table_prefix}_raw_ohlcv",
        temp_bucket=args.bucket_name
    )
    
    # Stream aggregates vào BigQuery
    agg_query = process_aggregates_to_bigquery(
        ohlcv_df=ohlcv_df,
        checkpoint_dir=GCS_CHECKPOINT,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_name=f"{args.table_prefix}_aggregates_ohlcv",
        temp_bucket=args.bucket_name,
        window_duration=args.window_duration
    )
    
    print("""
    ✅ Streaming queries started...
    Press Ctrl+C to stop
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
