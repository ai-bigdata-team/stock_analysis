# from google.cloud import bigquery
# import pandas as pd
#
# pd.set_option("display.max_columns", None)   # hiện tất cả cột
# pd.set_option("display.width", None)         # không xuống dòng gãy
# pd.set_option("display.max_colwidth", None)  # hiện full nội dung cell
#
# client = bigquery.Client(project="stockanalysis-480013")
#
# query = """
# SELECT *
# FROM `stockanalysis-480013.market_data.trades`
# """
#
# df = client.query(query).to_dataframe()
# print(df.head())
#
# # Tổng số dòng
# total_rows = len(df)
#
# # Số dòng có cột open khác NaN
# open_not_nan_rows = df["open"].notna().sum()
#
# print("Tổng số dòng:", total_rows)
# print("Số dòng open khác NaN:", open_not_nan_rows)
# distinct_rows = df.drop_duplicates().shape[0]
# print("Tổng số dòng distinct:", distinct_rows)

import os
import sys
import subprocess

# ===== Sử dụng Java 11 =====
JAVA_11_HOME = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot'

# ===== THÊM HADOOP_HOME =====
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin' + os.pathsep + os.environ.get('PATH', '')

java_exe = os.path.join(JAVA_11_HOME, 'bin', 'java.exe')
if not os.path.exists(java_exe):
    print(f"❌ ERROR: Không tìm thấy Java tại: {JAVA_11_HOME}")
    sys.exit(1)

os.environ['JAVA_HOME'] = JAVA_11_HOME
os.environ['PATH'] = os.path.join(JAVA_11_HOME, 'bin') + os.pathsep + os.environ['PATH']
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ===== Google Cloud credentials =====
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'F:\path\to\service-account-key.json'

print("🔍 Checking Java version...")
result = subprocess.run([java_exe, '-version'], capture_output=True, text=True)
print(result.stderr)

# Kiểm tra winutils
winutils_path = r'C:\hadoop\bin\winutils.exe'
if not os.path.exists(winutils_path):
    print(f"\n⚠️  WARNING: winutils.exe not found at {winutils_path}")
    print("   Download from: https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe")
    print("   And save to: C:\\hadoop\\bin\\winutils.exe")
    sys.exit(1)
else:
    print(f"✅ Found winutils.exe at {winutils_path}")

print("\n🚀 Starting PySpark with BigQuery connector...")
from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder \
        .appName("BigQuery to Spark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("\n✅ Spark session created successfully!")
    print(f"   Spark version: {spark.version}")

    # Đọc từ BigQuery
    print("\n📊 Reading from BigQuery...")

    df = spark.read \
        .format("bigquery") \
        .option("project", "stockanalysis-480013") \
        .option("dataset", "market_data") \
        .option("table", "trades") \
        .load()

    print("\n📋 Schema:")
    df.printSchema()

    print("\n📊 Sample data (first 20 rows):")
    df.show(20, truncate=False)

    print(f"\n📈 Total rows: {df.count()}")

    print("\n✅ Successfully loaded data from BigQuery!")

    # spark.stop()

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()