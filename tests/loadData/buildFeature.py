# # ============================================
# # 1. IMPORT
# # ============================================
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
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
# # ===== Google Cloud credentials =====
# # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'F:\path\to\service-account-key.json'
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
# except Exception as e:
#     print(f"\n❌ Error: {e}")
#     import traceback
#
#     traceback.print_exc()
# # ============================================
# # 2. LOAD DATA
# # ============================================
# df_raw = spark.read.parquet(
#     r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_new.parquet"
# )
#
# df = df_raw.orderBy("trade_timestamp")
#
# # ============================================
# # 3. DEFINE WINDOW FUNCTION (row-based)
# # ============================================
# def w(days):
#     return Window.orderBy("trade_timestamp").rowsBetween(-days, -1)
#
# # ============================================
# # 4. FEATURE OHLCV BASIC (17 features)
# # ============================================
#
# df = (
#     df.withColumn("prev_close", F.lag("close").over(Window.orderBy("trade_timestamp")))
#       .withColumn("close_open_spread", F.col("close") - F.col("open"))
#       .withColumn("high_low_spread", F.col("high") - F.col("low"))
#       .withColumn(
#           "true_range",
#           F.greatest(
#               F.col("high") - F.col("low"),
#               F.abs(F.col("high") - F.col("prev_close")),
#               F.abs(F.col("low") - F.col("prev_close"))
#           )
#       )
#       .withColumn("log_return", F.log(F.col("close") / F.col("prev_close")))
#       .withColumn("open_to_close_return", (F.col("close") - F.col("open")) / F.col("open"))
#       .withColumn("high_to_close_return", (F.col("high") - F.col("close")) / F.col("close"))
#       .withColumn("low_to_close_return", (F.col("low") - F.col("close")) / F.col("close"))
#       .withColumn("candle_body", F.col("close") - F.col("open"))
#       .withColumn("upper_shadow", F.col("high") - F.greatest("open","close"))
#       .withColumn("lower_shadow", F.least("open","close") - F.col("low"))
#       .withColumn("candle_body_ratio", F.abs(F.col("close") - F.col("open")) / (F.col("high") - F.col("low")))
#       .withColumn("volatility_ratio", (F.col("high") - F.col("low")) / F.col("close"))
#       .withColumn("overnight_gap", F.col("open") - F.col("prev_close"))
# )
#
# # Momentum features
# for d in [1, 3, 5, 10]:
#     df = df.withColumn(
#         f"momentum_{d}D",
#         (F.col("close") - F.lag("close", d).over(Window.orderBy("trade_timestamp")))
#         / F.lag("close", d).over(Window.orderBy("trade_timestamp"))
#     )
#
# # ============================================
# # 5. CREATE DIMS (DOW, DOM, TOD)
# # ============================================
#
# df = (
#     df.withColumn("dow", F.dayofweek("trade_timestamp"))
#       .withColumn("dom", F.dayofmonth("trade_timestamp"))
#       .withColumn("hour", F.hour("trade_timestamp"))
# )
#
# # DOW group
# df = (
#     df.withColumn("is_monday", (F.col("dow") == 2).cast("int"))
#       .withColumn("is_midweek", ((F.col("dow") >= 3) & (F.col("dow") <= 5)).cast("int"))
#       .withColumn("is_friday", (F.col("dow") == 6).cast("int"))
# )
#
# # DOM group
# df = (
#     df.withColumn("is_start_month", (F.col("dom") <= 10).cast("int"))
#       .withColumn("is_mid_month", ((F.col("dom") > 10) & (F.col("dom") <= 20)).cast("int"))
#       .withColumn("is_end_month", (F.col("dom") > 20).cast("int"))
# )
#
# # TOD group (6 buckets)
# df = (
#     df.withColumn("is_early_morning", ((F.col("hour") >= 0) & (F.col("hour") < 6)).cast("int"))
#       .withColumn("is_morning", ((F.col("hour") >= 6) & (F.col("hour") < 12)).cast("int"))
#       .withColumn("is_noon", ((F.col("hour") >= 12) & (F.col("hour") < 14)).cast("int"))
#       .withColumn("is_afternoon", ((F.col("hour") >= 14) & (F.col("hour") < 18)).cast("int"))
#       .withColumn("is_evening", ((F.col("hour") >= 18) & (F.col("hour") < 21)).cast("int"))
#       .withColumn("is_night", ((F.col("hour") >= 21)).cast("int"))
# )
#
# # ============================================
# # 6. LIST OF BASE FEATURES
# # ============================================
#
# base_features = [
#     "close_open_spread", "high_low_spread", "true_range", "log_return",
#     "open_to_close_return", "high_to_close_return", "low_to_close_return",
#     "candle_body", "upper_shadow", "lower_shadow", "candle_body_ratio",
#     "volatility_ratio", "overnight_gap",
#     "momentum_1D", "momentum_3D", "momentum_5D", "momentum_10D"
# ]
#
# # ============================================
# # 7. WEEKLY WINDOWS × DOW × TOD
# # ============================================
#
# weekly_windows = {
#     "L1W": 5,
#     "L2W": 10,
#     "L3W": 15
# }
#
# dim_DOW = ["is_monday", "is_midweek", "is_friday"]
# dim_TOD = ["is_early_morning","is_morning","is_noon",
#            "is_afternoon","is_evening","is_night"]
#
# for win_name, win_size in weekly_windows.items():
#     for dow in dim_DOW:
#         for tod in dim_TOD:
#             for f in base_features:
#                 df = df.withColumn(
#                     f"{f}_{win_name}_{dow}_{tod}",
#                     F.mean(f).over(w(win_size)) * F.col(dow) * F.col(tod)
#                 )
#
# # ============================================
# # 8. MONTHLY WINDOWS × DOM × TOD
# # ============================================
#
# monthly_windows = {
#     "L1M": 20,
#     "L3M": 60,
#     "L6M": 120,
#     "L12M": 240
# }
#
# dim_DOM = ["is_start_month","is_mid_month","is_end_month"]
#
# for win_name, win_size in monthly_windows.items():
#     for dom in dim_DOM:
#         for tod in dim_TOD:
#             for f in base_features:
#                 df = df.withColumn(
#                     f"{f}_{win_name}_{dom}_{tod}",
#                     F.mean(f).over(w(win_size)) * F.col(dom) * F.col(tod)
#                 )
#
# # ============================================
# # 9. DONE — SHOW SAMPLE
# # ============================================
#
# df.show(3)
# print("Total columns:", len(df.columns))
# # df.toPandas().to_parquet(
# #     r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_feature.parquet",
# #     engine="pyarrow",
# #     compression="snappy"
# # )


import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ============================================
# PATHS
# ============================================
INPUT_DIR = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_new_batch"
OUTPUT_DIR_WEEKLY = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_feature_batch_weekly"
OUTPUT_DIR_MONTHLY = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_feature_batch_monthly"

os.makedirs(OUTPUT_DIR_WEEKLY, exist_ok=True)
os.makedirs(OUTPUT_DIR_MONTHLY, exist_ok=True)

# ============================================
# PARAMS
# ============================================
weekly_windows = {"L1W": 5, "L2W": 10, "L3W": 15}
monthly_windows = {"L1M": 20, "L3M": 60, "L6M": 120, "L12M": 240}

dim_DOW = ["is_monday", "is_midweek", "is_friday"]
dim_DOM = ["is_start_month", "is_mid_month", "is_end_month"]
dim_TOD = [
    "is_early_morning", "is_morning", "is_noon",
    "is_afternoon", "is_evening", "is_night"
]

base_features = [
    "close_open_spread",
    "high_low_spread", "true_range", "log_return",
    "open_to_close_return", "high_to_close_return", "low_to_close_return",
    "candle_body", "upper_shadow", "lower_shadow", "candle_body_ratio",
    "volatility_ratio", "overnight_gap",
    "momentum_1D", "momentum_3D", "momentum_5D", "momentum_10D"
]

# ============================================
# TIME RANGES
# ============================================
# Weekly: Mondays from 2025-01-06 to 2025-11-03
start_date = datetime(2025, 1, 6)  # First Monday
end_date = datetime(2025, 11, 3)  # Last Monday before 2025-11-01
weekly_dates = []
current = start_date
while current <= end_date:
    weekly_dates.append(current)
    current += timedelta(days=7)

# Monthly: First day of each month from 2025-01-01 to 2025-11-01
monthly_dates = [datetime(2025, m, 1) for m in range(1, 12)]

print(f"📅 Weekly dates: {len(weekly_dates)} dates")
print(f"📅 Monthly dates: {len(monthly_dates)} dates")


# ============================================
# LABEL CALCULATION FUNCTION
# ============================================
def calculate_label(df, start_date, end_date):
    """
    Calculate label based on close price change from start to end of period
    Returns 1 if close price increased, 0 if decreased
    Returns None if insufficient data
    """
    df_period = df[(df["trade_timestamp"] >= start_date) &
                   (df["trade_timestamp"] < end_date)].copy()

    if len(df_period) < 2:
        return None

    # Get first and last close price in the period
    first_close = df_period.iloc[0]["close"]
    last_close = df_period.iloc[-1]["close"]

    # Label: 1 if price increased, 0 if decreased
    label = 1 if last_close > first_close else 0

    return label


# ============================================
# FEATURE CALCULATION FUNCTION
# ============================================
def calculate_features(df):
    """Calculate all features for a dataframe"""
    df = df.copy()
    df = df.sort_values("trade_timestamp").reset_index(drop=True)

    # Basic features
    df["prev_close"] = df["close"].shift(1)
    df["close_open_spread"] = df["close"] - df["open"]
    df["high_low_spread"] = df["high"] - df["low"]

    df["true_range"] = np.maximum.reduce([
        df["high"] - df["low"],
        abs(df["high"] - df["prev_close"]),
        abs(df["low"] - df["prev_close"])
    ])

    df["log_return"] = np.log(df["close"] / df["prev_close"])
    df["open_to_close_return"] = (df["close"] - df["open"]) / df["open"]
    df["high_to_close_return"] = (df["high"] - df["close"]) / df["close"]
    df["low_to_close_return"] = (df["low"] - df["close"]) / df["close"]

    df["candle_body"] = df["close"] - df["open"]
    df["upper_shadow"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["lower_shadow"] = df[["open", "close"]].min(axis=1) - df["low"]
    df["candle_body_ratio"] = abs(df["close"] - df["open"]) / (df["high"] - df["low"])
    df["volatility_ratio"] = (df["high"] - df["low"]) / df["close"]
    df["overnight_gap"] = df["open"] - df["prev_close"]

    for d in [1, 3, 5, 10]:
        shifted = df["close"].shift(d)
        df[f"momentum_{d}D"] = (df["close"] - shifted) / shifted

    # Time features
    df["dow"] = df["trade_timestamp"].dt.dayofweek + 1
    df["dom"] = df["trade_timestamp"].dt.day
    df["hour"] = df["trade_timestamp"].dt.hour

    df["is_monday"] = (df["dow"] == 1).astype(int)
    df["is_midweek"] = ((df["dow"] >= 2) & (df["dow"] <= 4)).astype(int)
    df["is_friday"] = (df["dow"] == 5).astype(int)

    df["is_start_month"] = (df["dom"] <= 10).astype(int)
    df["is_mid_month"] = ((df["dom"] > 10) & (df["dom"] <= 20)).astype(int)
    df["is_end_month"] = (df["dom"] > 20).astype(int)

    df["is_early_morning"] = (df["hour"] < 6).astype(int)
    df["is_morning"] = ((df["hour"] >= 6) & (df["hour"] < 12)).astype(int)
    df["is_noon"] = ((df["hour"] >= 12) & (df["hour"] < 14)).astype(int)
    df["is_afternoon"] = ((df["hour"] >= 14) & (df["hour"] < 18)).astype(int)
    df["is_evening"] = ((df["hour"] >= 18) & (df["hour"] < 21)).astype(int)
    df["is_night"] = (df["hour"] >= 21).astype(int)

    return df


def add_rolling_features(df, windows, dimensions, tod_dims):
    """Add rolling window features"""
    new_cols = {}

    for win_name, win_size in windows.items():
        for dim in dimensions:
            for tod in tod_dims:
                for f in base_features:
                    if f in df.columns:
                        roll = df[f].rolling(win_size, min_periods=1).mean().shift(1)
                        col_name = f"{f}_{win_name}_{dim}_{tod}"
                        new_cols[col_name] = roll * df[dim] * df[tod]

    # Concatenate all new columns at once
    if new_cols:
        new_df = pd.DataFrame(new_cols, index=df.index)
        df = pd.concat([df, new_df], axis=1)

    return df


def aggregate_features(df, stock_code, label):
    """Aggregate features to single row with label"""
    feature_cols = [c for c in df.columns if c not in
                    ["stock_code", "trade_timestamp", "open", "high", "low", "close", "volume",
                     "dow", "dom", "hour", "prev_close"]
                    ]

    if len(feature_cols) == 0:
        return None

    agg_dict = {c: "mean" for c in feature_cols}
    result_df = df.agg(agg_dict).to_frame().T
    result_df.insert(0, "stock_code", stock_code)
    result_df["label"] = label  # Add label column

    result_df.columns = [
        "_".join(col).strip("_") if isinstance(col, tuple) else col
        for col in result_df.columns
    ]

    return result_df


# ============================================
# PROCESS ALL FILES
# ============================================
files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".parquet")]
print(f"📂 Found {len(files)} parquet files\n")

for file in files:
    stock_code = file.replace(".parquet", "")
    print(f"🚀 Processing {stock_code}")

    # Create stock directories
    stock_weekly_dir = os.path.join(OUTPUT_DIR_WEEKLY, stock_code)
    stock_monthly_dir = os.path.join(OUTPUT_DIR_MONTHLY, stock_code)
    os.makedirs(stock_weekly_dir, exist_ok=True)
    os.makedirs(stock_monthly_dir, exist_ok=True)

    # Load data
    df = pd.read_parquet(os.path.join(INPUT_DIR, file))
    df["trade_timestamp"] = pd.to_datetime(df["trade_timestamp"])
    df = df.sort_values("trade_timestamp").reset_index(drop=True)

    print(f"  📊 Total records: {len(df)}")
    print(f"  📅 Date range: {df['trade_timestamp'].min()} to {df['trade_timestamp'].max()}")

    # ============================================
    # WEEKLY FEATURES
    # ============================================
    print(f"  ⏱️  Processing weekly features...")
    weekly_count = 0
    for i, txn_date in enumerate(weekly_dates):
        # Filter data before txn_date
        df_filtered = df[df["trade_timestamp"] < txn_date].copy()

        if len(df_filtered) < 10:  # Skip if not enough data
            continue

        # Calculate label for the week period
        # Week period: from txn_date to next Monday (7 days)
        period_end = txn_date + timedelta(days=7)
        label = calculate_label(df, txn_date, period_end)

        if label is None:  # Skip if no label can be calculated
            continue

        # Calculate features
        df_filtered = calculate_features(df_filtered)
        df_filtered = add_rolling_features(
            df_filtered, weekly_windows, dim_DOW, dim_TOD
        )

        # Aggregate
        result = aggregate_features(df_filtered, stock_code, label)
        if result is not None:
            output_path = os.path.join(
                stock_weekly_dir,
                f"{txn_date.strftime('%Y-%m-%d')}.parquet"
            )
            result.to_parquet(output_path, engine="pyarrow", compression="snappy")
            weekly_count += 1

    print(f"  ✅ Weekly: {weekly_count} files created")

    # ============================================
    # MONTHLY FEATURES
    # ============================================
    print(f"  ⏱️  Processing monthly features...")
    monthly_count = 0
    for i, txn_date in enumerate(monthly_dates):
        # Filter data before txn_date
        df_filtered = df[df["trade_timestamp"] < txn_date].copy()

        if len(df_filtered) < 20:  # Skip if not enough data
            continue

        # Calculate label for the month period
        # Month period: from txn_date to first day of next month
        if txn_date.month == 12:
            period_end = datetime(txn_date.year + 1, 1, 1)
        else:
            period_end = datetime(txn_date.year, txn_date.month + 1, 1)

        label = calculate_label(df, txn_date, period_end)

        if label is None:  # Skip if no label can be calculated
            continue

        # Calculate features
        df_filtered = calculate_features(df_filtered)
        df_filtered = add_rolling_features(
            df_filtered, monthly_windows, dim_DOM, dim_TOD
        )

        # Aggregate
        result = aggregate_features(df_filtered, stock_code, label)
        if result is not None:
            output_path = os.path.join(
                stock_monthly_dir,
                f"{txn_date.strftime('%Y-%m-%d')}.parquet"
            )
            result.to_parquet(output_path, engine="pyarrow", compression="snappy")
            monthly_count += 1

    print(f"  ✅ Monthly: {monthly_count} files created")
    print(f"  ✨ Completed {stock_code}\n")

print("🎉 All done!")
