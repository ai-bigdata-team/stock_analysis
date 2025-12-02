import argparse
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date,
    when, expr, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType


def create_spark_session(iceberg_warehouse, gcs_key_path):
    """Create Spark session with Iceberg and GCS support"""
    print("Creating Spark session with Iceberg...")
    
    spark = SparkSession.builder \
        .appName("VNStock_Iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", iceberg_warehouse) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key_path) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("Spark session created")
    return spark


def create_iceberg_database(spark):
    """Create Iceberg database for VNStock"""
    print("Setting up Iceberg database...")
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.vnstock")
    spark.sql("USE iceberg_catalog.vnstock")
    
    print("Database ready: iceberg_catalog.vnstock")


def create_daily_ohlcv_table(spark):
    """Create Iceberg table for daily OHLCV data"""
    print("Creating daily_ohlcv table...")
    
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS iceberg_catalog.vnstock.daily_ohlcv (
            symbol STRING,
            trade_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            value DOUBLE,
            price_change DOUBLE,
            price_change_pct DOUBLE,
            data_source STRING,
            ingestion_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (trade_date, symbol)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.metadata.compression-codec' = 'gzip',
            'write.target-file-size-bytes' = '134217728'
        )
    """
    
    spark.sql(create_table_sql)
    print("daily_ohlcv table ready")


def create_daily_summary_table(spark):
    """Create Iceberg table for daily market summary"""
    print("Creating daily_summary table...")
    
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS iceberg_catalog.vnstock.daily_summary (
            trade_date DATE,
            total_symbols INT,
            total_volume BIGINT,
            total_value DOUBLE,
            gainers INT,
            losers INT,
            unchanged INT,
            avg_price_change_pct DOUBLE,
            max_price_change_pct DOUBLE,
            min_price_change_pct DOUBLE,
            top_gainer STRING,
            top_loser STRING,
            computed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (trade_date)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
    """
    
    spark.sql(create_table_sql)
    print("daily_summary table ready")


def load_and_transform_data(spark, temp_data_path, execution_date):
    """Load data from temp path and transform"""
    print(f"Loading data from {temp_data_path}...")
    
    # Read parquet file
    df = spark.read.parquet(temp_data_path)
    
    print(f"  Records loaded: {df.count()}")
    print("  Schema:")
    df.printSchema()
    
    # Transform data
    transformed_df = df \
        .withColumnRenamed("time", "trade_date") \
        .withColumnRenamed("ticker", "symbol") \
        .withColumn("trade_date", to_date(col("trade_date"))) \
        .withColumn("price_change", col("close") - col("open")) \
        .withColumn("price_change_pct", 
                   when(col("open") != 0, 
                        spark_round((col("close") - col("open")) / col("open") * 100, 2))
                   .otherwise(0)) \
        .withColumn("value", col("close") * col("volume")) \
        .withColumn("data_source", lit("vnstock_vci")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .select(
            "symbol",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            col("volume").cast("bigint"),
            "value",
            "price_change",
            "price_change_pct",
            "data_source",
            "ingestion_timestamp"
        )
    
    print("Data transformed")
    return transformed_df


def write_to_iceberg(df, execution_date):
    """Write data to Iceberg table"""
    print(f"Writing to Iceberg table for {execution_date}...")
    
    # Upsert mode: overwrite partition for the execution date
    df.writeTo("iceberg_catalog.vnstock.daily_ohlcv") \
        .using("iceberg") \
        .tableProperty("write.spark.accept-any-schema", "true") \
        .overwritePartitions()
    
    count = df.count()
    print(f"Written {count} records to daily_ohlcv")
    
    return count


def compute_daily_summary(spark, execution_date):
    """Compute and write daily market summary"""
    print(f"Computing daily summary for {execution_date}...")
    
    summary_sql = f"""
        SELECT
            trade_date,
            COUNT(DISTINCT symbol) as total_symbols,
            SUM(volume) as total_volume,
            SUM(value) as total_value,
            SUM(CASE WHEN price_change > 0 THEN 1 ELSE 0 END) as gainers,
            SUM(CASE WHEN price_change < 0 THEN 1 ELSE 0 END) as losers,
            SUM(CASE WHEN price_change = 0 THEN 1 ELSE 0 END) as unchanged,
            ROUND(AVG(price_change_pct), 2) as avg_price_change_pct,
            ROUND(MAX(price_change_pct), 2) as max_price_change_pct,
            ROUND(MIN(price_change_pct), 2) as min_price_change_pct,
            FIRST_VALUE(symbol) OVER (ORDER BY price_change_pct DESC) as top_gainer,
            FIRST_VALUE(symbol) OVER (ORDER BY price_change_pct ASC) as top_loser,
            CURRENT_TIMESTAMP() as computed_at
        FROM iceberg_catalog.vnstock.daily_ohlcv
        WHERE trade_date = DATE'{execution_date}'
        GROUP BY trade_date
    """
    
    summary_df = spark.sql(summary_sql)
    
    # Write to summary table
    summary_df.writeTo("iceberg_catalog.vnstock.daily_summary") \
        .using("iceberg") \
        .overwritePartitions()
    
    print("Daily summary computed and saved")
    
    # Show summary
    print("\nMarket Summary:")
    summary_df.show(truncate=False)
    
    return summary_df


def cleanup_old_snapshots(spark, retention_days=7):
    """Cleanup old Iceberg snapshots to save storage"""
    print(f"Cleaning up snapshots older than {retention_days} days...")
    
    # Expire old snapshots
    spark.sql(f"""
        CALL iceberg_catalog.system.expire_snapshots(
            table => 'vnstock.daily_ohlcv',
            older_than => TIMESTAMP'{datetime.now()}' - INTERVAL {retention_days} DAYS,
            retain_last => 3
        )
    """)
    
    print("Snapshot cleanup complete")


def get_args():
    parser = argparse.ArgumentParser(description="VNStock Batch Processing to Iceberg")
    parser.add_argument("--execution-date", required=True, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--temp-data-path", required=True, help="Temporary data path")
    parser.add_argument("--iceberg-warehouse", required=True, help="Iceberg warehouse path")
    parser.add_argument("--gcs-key", default=os.environ.get("GCS_JSON_KEY", "~/gcs_bigdatastockanalysis.json"),
                       help="GCS service account key path")
    return parser.parse_args()


def main():
    """Main execution"""
    args = get_args()
    
    print("\n" + "="*70)
    print("VNSTOCK BATCH PROCESSING TO ICEBERG")
    print("="*70)
    print(f"Execution Date: {args.execution_date}")
    print(f"Temp Data: {args.temp_data_path}")
    print(f"Iceberg Warehouse: {args.iceberg_warehouse}")
    print("="*70 + "\n")
    
    # Expand GCS key path
    gcs_key_path = os.path.expanduser(args.gcs_key)
    
    # Create Spark session
    spark = create_spark_session(args.iceberg_warehouse, gcs_key_path)
    
    try:
        # Setup Iceberg
        create_iceberg_database(spark)
        create_daily_ohlcv_table(spark)
        create_daily_summary_table(spark)
        
        # Process data
        df = load_and_transform_data(spark, args.temp_data_path, args.execution_date)
        record_count = write_to_iceberg(df, args.execution_date)
        
        # Compute summary
        compute_daily_summary(spark, args.execution_date)
        
        # Cleanup old snapshots
        cleanup_old_snapshots(spark, retention_days=7)
        
        print("\n" + "="*70)
        print("BATCH PROCESSING COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"Records processed: {record_count}")
        print(f"Iceberg tables updated:")
        print(f"  - vnstock.daily_ohlcv")
        print(f"  - vnstock.daily_summary")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
