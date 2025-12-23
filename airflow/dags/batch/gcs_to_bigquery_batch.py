"""
GCS to BigQuery Batch Loader
============================
Đọc dữ liệu từ GCS (Parquet) và load vào BigQuery
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ================== ENV & GLOBAL CONFIG ==================

load_dotenv()

# Service account key
gcs_key_path = os.path.expanduser(os.environ["GCS_JSON_KEY"])

# JARs
gcs_jar_path = "jars/gcs/gcs-connector-shaded.jar"
bigquery_jar_path = "jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar"

# BigQuery config
PROJECT_ID = "stockanalysis-480013"
DATASET_ID = "stock_data_nnminh"
TABLE_PREFIX = "vnstock"

# GCS config
TEMP_BUCKET = "stock_data_hehehe"
GCS_BASE_PATH = "gs://stock_data_hehehe/vnstock_data"
RAW_OHLCV_PATH = f"{GCS_BASE_PATH}/raw/ohlcv"


# ================== SPARK SESSION ==================

def create_spark_session(gcs_key_path):
    return (
        SparkSession.builder
        .master("spark://localhost:7077")
        .appName("GCS_to_BigQuery_Loader")
        .config("spark.jars", f"{gcs_jar_path},{bigquery_jar_path}")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key_path)
        .config("spark.hadoop.fs.gs.project.id", PROJECT_ID)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )


# ================== LOAD RAW ==================

def load_raw_ohlcv_to_bigquery(spark):
    print(f"\nReading RAW data from: {RAW_OHLCV_PATH}")

    # Refresh metadata cache for the path
    try:
        print(f"Refreshing metadata cache...")
        spark.catalog.refreshByPath(RAW_OHLCV_PATH)
    except Exception as e:
        print(f"Could not refresh path (might be first time): {str(e)}")

    try:
        df = spark.read.parquet(RAW_OHLCV_PATH)
    except Exception as e:
        print(f"No data found or error reading: {str(e)}")
        return

    if df.rdd.isEmpty():
        print("No records found in RAW path. Skipping...")
        return

    print(f"Total records read from GCS: {df.count()}")
    
    # STEP 1: Deduplicate data
    print(f"\nDeduplicating records...")
    if "record_id" in df.columns:
        df_deduped = df.dropDuplicates(["record_id"])
        print(f"   Deduplicated by: record_id")
    else:
        df_deduped = df.dropDuplicates(["symbol", "trade_timestamp"])
        print(f"   Deduplicated by: symbol + trade_timestamp")
    
    duplicates_removed = df.count() - df_deduped.count()
    print(f"Removed {duplicates_removed} duplicate records")
    print(f"Unique records: {df_deduped.count()}")
    df = df_deduped
    
    # STEP 2: Filter for last 5 days data
    now = datetime.now()
    
    # Get end date (handle weekends)
    end_date = now
    if now.weekday() == 5:  # Saturday
        end_date = now - timedelta(days=1)
        print(f"\nToday is Saturday, using Friday as end date")
    elif now.weekday() == 6:  # Sunday
        end_date = now - timedelta(days=2)
        print(f"\nToday is Sunday, using Friday as end date")
    else:
        print(f"\nLoading data for last 5 days")
    
    # Get start date (5 days ago)
    start_date = now - timedelta(days=5)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"   Date range: {start_date_str} to {end_date_str} (5 days)")
    print(f"   End date weekday: {end_date.strftime('%A')}")
    
    # Add trade_date column if not exists
    if "trade_date" not in df.columns:
        df = df.withColumn("trade_date", to_date(col("trade_timestamp")))
    
    # Filter for last 5 days
    df_filtered = df.filter(
        (col("trade_date") >= start_date_str) & 
        (col("trade_date") <= end_date_str)
    )
    records_before = df.count()
    records_after = df_filtered.count()
    
    print(f"   Records before date filter: {records_before}")
    print(f"   Records after date filter: {records_after}")
    print(f"   Filtered out: {records_before - records_after} records")
    
    if records_after == 0:
        print(f"No records found for date range {start_date_str} to {end_date_str}. Skipping...")
        return
    
    df = df_filtered
    
    # Add year and month columns
    if "year" not in df.columns:
        df = df.withColumn("year", year(col("trade_timestamp")))
    if "month" not in df.columns:
        df = df.withColumn("month", month(col("trade_timestamp")))

    print(f"\nFinal records to load: {df.count()}")
    df.printSchema()

    target_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_PREFIX}_raw_ohlcv"
    print(f"\nWriting {df.count()} records to BigQuery: {target_table}")

    (
        df.write
        .format("bigquery")
        .option("table", target_table)
        .option("project", PROJECT_ID)
        .option("parentProject", PROJECT_ID)
        .option("credentialsFile", gcs_key_path)
        .option("temporaryGcsBucket", TEMP_BUCKET)
        .option("writeMethod", "direct")
        .option("partitionField", "trade_date")
        .option("partitionType", "DAY")
        .option("clusteredFields", "symbol")
        .mode("append")
        .save()
    )

    print("RAW load done")


# ================== MAIN ==================

def main():
    if not os.path.exists(gcs_key_path):
        raise FileNotFoundError(f"GCS key file not found: {gcs_key_path}")

    # Load interval: 1 day = 86400 seconds
    LOAD_INTERVAL = int(os.getenv("GCS_TO_BQ_INTERVAL", "43200"))  # 12 hours
    
    print(f"""
    =====================================
    GCS → BigQuery Daily Loader
    =====================================
    Source: {RAW_OHLCV_PATH}
    Target: {PROJECT_ID}.{DATASET_ID}.{TABLE_PREFIX}_raw_ohlcv
    Load Interval: {LOAD_INTERVAL} seconds ({LOAD_INTERVAL/3600:.1f} hours)
    =====================================
    """)

    spark = create_spark_session(gcs_key_path)
    spark.sparkContext.setLogLevel("WARN")

    run_count = 0
    
    try:
        while True:
            run_count += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n{'='*70}")
            print(f"Daily Load Run #{run_count}")
            print(f"Time: {timestamp} ({datetime.now().strftime('%A')})")
            print(f"{'='*70}")
            
            try:
                load_raw_ohlcv_to_bigquery(spark)
                print(f"\nLoad completed successfully")
            except Exception as e:
                print(f"\nError during load: {str(e)}")
                print("Will retry on next scheduled run...")
            
            next_run_time = datetime.now() + timedelta(seconds=LOAD_INTERVAL)
            print(f"\nNext daily load in {LOAD_INTERVAL} seconds ({LOAD_INTERVAL/3600:.1f} hours)")
            print(f"   Next run at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} ({next_run_time.strftime('%A')})")
            print(f"{'='*70}\n")
            
            time.sleep(LOAD_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\nStopped by user (Ctrl+C)")
        print(f"Total runs completed: {run_count}")
    finally:
        spark.stop()
        print("Spark session closed")


if __name__ == "__main__":
    main()
