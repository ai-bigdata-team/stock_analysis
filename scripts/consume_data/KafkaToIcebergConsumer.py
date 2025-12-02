from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import json

# 1. Tạo Spark session (bật Iceberg)
spark = SparkSession.builder \
    .appName("KafkaToIcebergConsumer") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "warehouse/iceberg") \
    .getOrCreate()

# 2. Khởi tạo Kafka Consumer
consumer = KafkaConsumer(
    'vnstock_stock',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

# 3. Định nghĩa schema
schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True),
    StructField("ticker", StringType(), True)
])

# 4. Đọc từ Kafka và ghi vào Iceberg
for msg in consumer:
    record = msg.value
    df = spark.createDataFrame([record], schema=schema)
    df.writeTo("local.db.vnstock_prices").append()
    print(f"✅ Inserted record for {record['ticker']} into Iceberg table.")
