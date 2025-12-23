# # from google.cloud import bigquery
# # import pandas as pd
# #
# # pd.set_option("display.max_columns", None)   # hiện tất cả cột
# # pd.set_option("display.width", None)         # không xuống dòng gãy
# # pd.set_option("display.max_colwidth", None)  # hiện full nội dung cell
# #
# # client = bigquery.Client(project="stockanalysis-480013")
# #
# # query = """
# # SELECT *
# # FROM `stockanalysis-480013.market_data.trades`
# # """
# #
# # df = client.query(query).to_dataframe()
# # print(df.head())
# #
# # # Tổng số dòng
# # total_rows = len(df)
# #
# # # Số dòng có cột open khác NaN
# # open_not_nan_rows = df["open"].notna().sum()
# #
# # print("Tổng số dòng:", total_rows)
# # print("Số dòng open khác NaN:", open_not_nan_rows)
# # distinct_rows = df.drop_duplicates().shape[0]
# # print("Tổng số dòng distinct:", distinct_rows)
#
# import os
# import sys
# import subprocess
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
#
# # ===== Sử dụng Java 11 =====
# JAVA_11_HOME = r'C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot'
#
# # ===== THÊM HADOOP_HOME =====
# os.environ['HADOOP_HOME'] = r'C:\hadoop'
# os.environ['PATH'] = r'C:\hadoop\bin' + os.pathsep + os.environ.get('PATH', '')
#
# java_exe = os.path.join(JAVA_11_HOME, 'bin', 'java.exe')
# if not os.path.exists(java_exe):
#     print(f"❌ ERROR: Không tìm thấy Java tại: {JAVA_11_HOME}")
#     sys.exit(1)
#
# os.environ['JAVA_HOME'] = JAVA_11_HOME
# os.environ['PATH'] = os.path.join(JAVA_11_HOME, 'bin') + os.pathsep + os.environ['PATH']
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#
# print("🔍 Checking Java version...")
# result = subprocess.run([java_exe, '-version'], capture_output=True, text=True)
# print(result.stderr)
#
# # Kiểm tra winutils
# winutils_path = r'C:\hadoop\bin\winutils.exe'
# if not os.path.exists(winutils_path):
#     print(f"\n⚠️  WARNING: winutils.exe not found at {winutils_path}")
#     print("   Download from: https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe")
#     print("   And save to: C:\\hadoop\\bin\\winutils.exe")
#     sys.exit(1)
# else:
#     print(f"✅ Found winutils.exe at {winutils_path}")
#
# print("\n🚀 Starting PySpark with BigQuery connector...")
# from pyspark.sql import SparkSession
#
# try:
#     spark = SparkSession.builder \
#         .appName("BigQuery to Spark") \
#         .master("local[*]") \
#         .config("spark.driver.memory", "4g") \
#         .config("spark.hadoop.io.native.lib.available", "false") \
#         .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
#         .config("spark.hadoop.fs.defaultFS", "file:///") \
#         .getOrCreate()
#
#     spark.sparkContext.setLogLevel("ERROR")
#
#     print("\n✅ Spark session created successfully!")
#     print(f"   Spark version: {spark.version}")
#
#     # Đọc từ BigQuery
#     print("\n📊 Reading from BigQuery...")
#
#     df = spark.read \
#         .format("bigquery") \
#         .option("project", "stockanalysis-480013") \
#         .option("dataset", "foreign_stock") \
#         .option("table", "tiingo") \
#         .load()
#
#     print("\n📊 Thành công")
#     df_spark = df.withColumnRenamed("symbol", "stock_code") \
#         .withColumnRenamed("date", "trade_timestamp")
#
#     # Chuyển timestamp sang định dạng chuẩn
#     df_spark = df_spark.withColumn("trade_timestamp", F.to_timestamp("trade_timestamp"))
#
#     # ===== Sinh các chỉ số tài chính =====
#     df_spark = df_spark.withColumn(
#         "base_eps",
#         (F.col("close") % 4000 + 1000)
#     )
#
#     df_spark = df_spark.withColumn(
#         "EPS",
#         F.round(
#             F.col("base_eps") * (0.9 + F.rand() * 0.2),
#             0
#         )
#     )
#
#     df_spark = df_spark.withColumn(
#         "PE",
#         F.round(F.col("close") * 1000 / F.col("EPS"), 2)
#     )
#
#     df_spark = df_spark.withColumn("PB", F.round(1.0 + F.rand() * 4.0, 2))
#     df_spark = df_spark.withColumn("ROE", F.round(10.0 + F.rand() * 20.0, 2))
#     df_spark = df_spark.withColumn("ROA", F.round(5.0 + F.rand() * 10.0, 2))
#     df_spark = df_spark.withColumn("Beta", F.round(0.5 + F.rand() * 2.0, 2))
#     df_spark = df_spark.withColumn(
#         "MarketCap",
#         F.col("close") * F.col("volume") * 100
#     )
#
#     # Xóa cột tạm
#     df_spark = df_spark.drop("base_eps")
#
#     # Select các cột cần thiết
#     df_final = df_spark.select(
#         "trade_timestamp", "stock_code",
#         "open", "high", "low", "close", "volume",
#         "EPS", "PE", "PB", "ROE", "ROA", "Beta", "MarketCap"
#     )
#
#     print("\n📋 Schema:")
#     df_final.printSchema()
#
#     print("\n📊 Sample data (first 20 rows):")
#     df_final.show(20, truncate=False)
#
#     print(f"\n📈 Total rows: {df_final.count()}")
#
#     # Tạo thư mục output nếu chưa có
#     output_dir = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_new_batch"
#     os.makedirs(output_dir, exist_ok=True)
#
#     # Lấy danh sách các stock_code duy nhất
#     stock_codes = df_final.select("stock_code").distinct().collect()
#     stock_codes = [row.stock_code for row in stock_codes]
#
#     print(f"\n📦 Found {len(stock_codes)} unique stock codes")
#     print(f"🔄 Processing each stock code...")
#
#     # Lưu từng stock_code vào file riêng
#     for i, stock_code in enumerate(stock_codes, 1):
#         print(f"  [{i}/{len(stock_codes)}] Processing {stock_code}...", end=" ")
#
#         # Filter data cho stock_code này
#         df_stock = df_final.filter(F.col("stock_code") == stock_code)
#
#         # Chuyển sang Pandas và lưu
#         df_pandas = df_stock.toPandas()
#         df_pandas["trade_timestamp"] = df_pandas["trade_timestamp"].astype("datetime64[us]")
#
#         # Đường dẫn file output
#         output_path = os.path.join(output_dir, f"{stock_code}.parquet")
#
#         df_pandas.to_parquet(
#             output_path,
#             engine="pyarrow",
#             compression="snappy"
#         )
#
#         print(f"✅ Saved {len(df_pandas)} rows")
#
#     print(f"\n✅ Successfully saved {len(stock_codes)} files to {output_dir}")
#
#     spark.stop()
#
# except Exception as e:
#     print(f"\n❌ Error: {e}")
#     import traceback
#
#     traceback.print_exc()
#
#     traceback.print_exc()


import os
import pandas as pd

# Đường dẫn file input và thư mục output
input_file = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_new.parquet"
output_dir = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_new_batch"

# Tạo thư mục output nếu chưa có
os.makedirs(output_dir, exist_ok=True)

print("📊 Reading parquet file...")
# Đọc file parquet
df = pd.read_parquet(input_file, engine="pyarrow")

print(f"✅ Loaded {len(df)} rows")
print(f"📋 Columns: {list(df.columns)}")
print(f"\n📊 Sample data:")
print(df.head())

# Lấy danh sách stock_code duy nhất
stock_codes = df['stock_code'].unique()
print(f"\n📦 Found {len(stock_codes)} unique stock codes")

# Lưu từng stock_code vào file riêng
print(f"🔄 Processing each stock code...")
for i, stock_code in enumerate(stock_codes, 1):
    print(f"  [{i}/{len(stock_codes)}] Processing {stock_code}...", end=" ")

    # Filter data cho stock_code này
    df_stock = df[df['stock_code'] == stock_code].copy()

    # Đường dẫn file output
    output_path = os.path.join(output_dir, f"{stock_code}.parquet")

    # Lưu file
    df_stock.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    print(f"✅ Saved {len(df_stock)} rows")

print(f"\n✅ Successfully saved {len(stock_codes)} files to {output_dir}")