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

# ================== CONFIG ==================
GCS_BUCKET = "stock_data_hehehe"
GCS_BASE_PATH = f"gs://{GCS_BUCKET}/vnstock_data"
GCS_CHECKPOINT = f"gs://{GCS_BUCKET}/checkpoints/vnstock"

gcs_key_path = os.path.expanduser(os.environ["GCS_JSON_KEY"])
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vnstock_stock"

gcs_jar_path = "/mnt/d/Project/stock_analysis/jars/gcs/gcs-connector-shaded.jar"
bigquery_jar_path = "/mnt/d/Project/stock_analysis/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar"

# ---------------- SPARK SESSION ----------------
def create_spark_session(gcs_key_path):
    return SparkSession.builder \
        .master("spark://localhost:7077") \
        .appName("VNStockKafkaToGCS") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.jars",gcs_jar_path + "," + bigquery_jar_path) \
        .config("spark.hadoop.fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key_path) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "6") \
        .config("spark.default.parallelism", "6") \
        .getOrCreate()

# ---------------- VNSTOCK SCHEMA ----------------
def get_vnstock_schema():
    return StructType([
        StructField("ticker", StringType(), True),  # Kafka dùng ticker
        StructField("time", StringType(), True),    # ISO string
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True)
    ])

# ---------------- RAW OHLCV ----------------
def process_raw_ohlcv(df, checkpoint_dir, output_path):
    print("\n🔄 Setting up RAW OHLCV processing...")

    ohlcv_df = df.withColumnRenamed("ticker", "symbol") \
                 .withColumn("trade_timestamp", to_timestamp(col("time"))) \
                 .select(
                     "symbol",
                     "trade_timestamp",
                     "open",
                     "high",
                     "low",
                     "close",
                     "volume",
                     (col("high") - col("low")).alias("price_range"),
                     (col("close") - col("open")).alias("price_change"),
                     ((col("close") - col("open")) / col("open") * 100).alias("price_change_pct"),
                     expr("uuid()").alias("record_id"),
                     current_timestamp().alias("ingest_timestamp")
                 ) \
                 .filter(col("trade_timestamp").isNotNull()) \
                 .withWatermark("trade_timestamp", "0 seconds")  # test nhanh

    print("📊 Raw OHLCV DataFrame Schema:")
    ohlcv_df.printSchema()

    query = ohlcv_df.writeStream \
        .queryName("VNStock_Raw_OHLCV_to_GCS") \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_dir}/raw_ohlcv") \
        .option("path", f"{output_path}/raw/ohlcv") \
        .partitionBy("symbol") \
        .trigger(processingTime="30 seconds") \
        .option("maxRecordsPerFile", "50000") \
        .start()

    print(f"✅ Raw OHLCV query started - output: {output_path}/raw/ohlcv")
    return query

# ---------------- MAIN ----------------
def main():
    print("\n🔌 Creating Spark session...")
    spark = create_spark_session(gcs_key_path)
    spark.sparkContext.setLogLevel("WARN")
    
    # Check Spark connection
    print(f"\n{'='*60}")
    print("🔍 SPARK CONNECTION STATUS")
    print(f"{'='*60}")
    try:
        master_url = spark.sparkContext.master
        app_id = spark.sparkContext.applicationId
        ui_url = spark.sparkContext.uiWebUrl
        
        print(f"✅ Spark Master: {master_url}")
        print(f"📱 Application ID: {app_id}")
        print(f"🌐 Spark UI: {ui_url}")
        print(f"🔢 Default Parallelism: {spark.sparkContext.defaultParallelism}")
        
        # Try to get executor info
        try:
            executor_info = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
            executor_count = executor_info.size()
            print(f"⚡ Active Executors: {executor_count}")
            if executor_count == 0:
                print("⚠️  WARNING: No executors available! Check cluster status.")
                print(f"   Cluster UI: http://localhost:8081")
        except Exception as e:
            print(f"⚠️  Could not get executor info: {str(e)}")
            
    except Exception as e:
        print(f"❌ Error checking Spark connection: {str(e)}")
    print(f"{'='*60}\n")

    print(f"""
    ================================
    VNStock → GCS Pipeline
    ================================
    Kafka: {KAFKA_BOOTSTRAP_SERVERS} → {KAFKA_TOPIC}
    GCS Output: {GCS_BASE_PATH}/raw/ohlcv
    ================================
    """)

    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    schema = get_vnstock_schema()
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("parsed_data")) \
                        .select("parsed_data.*")

    raw_query = process_raw_ohlcv(parsed_df, GCS_CHECKPOINT, GCS_BASE_PATH)

    print("\n✅ Streaming started - Press Ctrl+C to stop\n")
    
    try:
        raw_query.awaitTermination()
    except KeyboardInterrupt:
        raw_query.stop()
        print("\n✅ Streaming stopped gracefully")

if __name__ == "__main__":
    main()
