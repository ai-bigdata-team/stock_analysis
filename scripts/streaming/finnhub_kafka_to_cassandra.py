"""
PySpark Structured Streaming job: Kafka → Transform → Cassandra/Parquet
Đây là phiên bản Python tương đương với StreamProcessor.scala
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, current_timestamp, 
    avg, window, udf
)
from pyspark.sql.types import StringType
import uuid

# Import module xử lý của chúng ta
from finnhub_processing import schema, process_stocktrade_data_realtime


def create_spark_session(app_name="Finnhub-Kafka-Streaming"):
    """Tạo Spark Session với cấu hình cần thiết"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()


def read_from_kafka(spark, kafka_servers="localhost:9092", topic="finnhub_stock"):
    """Đọc streaming data từ Kafka"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .load()


def parse_kafka_messages(df, schema):
    """
    Parse JSON messages từ Kafka value column.
    Giả sử message format: {"data": [{"p":123, "s":"AAPL", ...}], "type":"trade"}
    """
    # Parse JSON từ value column
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("parsed_data")
    )
    
    # Explode array "data" để có từng trade riêng lẻ
    exploded_df = parsed_df.select(
        explode(col("parsed_data.data")).alias("trade")
    )
    
    # Extract các trường từ trade
    trades_df = exploded_df.select(
        col("trade.c").alias("c"),
        col("trade.p").alias("p"),
        col("trade.s").alias("s"),
        col("trade.t").alias("t"),
        col("trade.v").alias("v")
    )
    
    return trades_df


def add_uuid_column(df):
    """Thêm UUID column cho Cassandra primary key"""
    uuid_udf = udf(lambda: str(uuid.uuid1()), StringType())
    return df.withColumn("uuid", uuid_udf())


def write_to_cassandra(df, keyspace="stock_data", table="trades"):
    """Ghi streaming data vào Cassandra"""
    return df \
        .writeStream \
        .foreachBatch(lambda batch_df, batch_id: 
            batch_df.write
                .format("org.apache.spark.sql.cassandra")
                .options(table=table, keyspace=keyspace)
                .mode("append")
                .save()
        ) \
        .outputMode("update") \
        .start()


def write_to_parquet(df, output_path, checkpoint_path):
    """Ghi streaming data vào Parquet files (alternative to Cassandra)"""
    return df \
        .writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()


def create_aggregates(df):
    """
    Tạo aggregates: tính giá trung bình theo symbol mỗi 15 giây
    Tương tự như summaryDF trong Scala version
    """
    return df \
        .withColumn("price_volume_multiply", col("price") * col("volume")) \
        .withWatermark("event_time", "15 seconds") \
        .groupBy("symbol", window("event_time", "15 seconds")) \
        .agg(avg("price_volume_multiply").alias("avg_price_volume"))


def main():
    """Main function - orchestrate toàn bộ pipeline"""
    
    # 1. Tạo Spark session
    print("🚀 Starting Spark Streaming Job...")
    spark = create_spark_session()
    
    # 2. Đọc từ Kafka
    print("📥 Reading from Kafka...")
    kafka_df = read_from_kafka(
        spark, 
        kafka_servers="localhost:9092",
        topic="finnhub_stock"
    )
    
    # 3. Parse JSON messages
    print("🔧 Parsing Kafka messages...")
    # Cần định nghĩa schema cho message wrapper
    from pyspark.sql.types import StructType, StructField, ArrayType
    
    message_schema = StructType([
        StructField("data", ArrayType(schema), True),
        StructField("type", StringType(), True)
    ])
    
    trades_df = parse_kafka_messages(kafka_df, message_schema)
    
    # 4. Transform data với finnhub_processing module
    print("⚙️ Transforming data...")
    transformed_df = process_stocktrade_data_realtime(trades_df)
    
    # Thêm ingest timestamp và UUID
    final_df = transformed_df \
        .withColumn("ingest_timestamp", current_timestamp()) \
        .withColumn("uuid", udf(lambda: str(uuid.uuid1()), StringType())())
    
    # 5. Tạo aggregates
    print("📊 Creating aggregates...")
    aggregates_df = create_aggregates(final_df)
    
    # 6A. Ghi vào Parquet (dễ test hơn Cassandra)
    print("💾 Writing to Parquet...")
    query1 = write_to_parquet(
        final_df,
        output_path="./output/trades",
        checkpoint_path="./checkpoints/trades"
    )
    
    query2 = write_to_parquet(
        aggregates_df,
        output_path="./output/aggregates",
        checkpoint_path="./checkpoints/aggregates"
    )
    
    # 6B. Hoặc ghi vào Cassandra (uncomment nếu có Cassandra)
    # query1 = write_to_cassandra(final_df, table="trades")
    # query2 = write_to_cassandra(aggregates_df, table="aggregates")
    
    # 7. Đợi job chạy
    print("✅ Streaming job is running...")
    print("📍 Check output at: ./output/trades and ./output/aggregates")
    print("🛑 Press Ctrl+C to stop")
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
