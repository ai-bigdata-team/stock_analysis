import logging
from typing import Union, Any
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import (
    StructType, 
    StructField, 
    LongType,
    FloatType,
    IntegerType,
    StringType,
    ArrayType,
    TimestampType
)



schema = StructType([
    StructField("c", ArrayType(StringType()), True),
    StructField("p", FloatType(), True),
    StructField("s", StringType(), True),
    StructField("t", LongType(), True),
    StructField("v", IntegerType(), True),
])




def convert_timestamp_to_datetime(timestamp_value, unit='ms'):
    if unit == 'ms':
        return datetime.fromtimestamp(timestamp_value / 1000)
    else:  # unit == 's'
        return datetime.fromtimestamp(timestamp_value)


def convert_timestamp_column(df: DataFrame, timestamp_col: str = "t", output_col: str = "event_time") -> DataFrame:

    # Chia cho 1000 để chuyển từ milliseconds sang seconds, sau đó cast sang timestamp
    return df.withColumn(
        output_col,
        (col(timestamp_col) / 1000).cast(TimestampType())
    )


def process_stocktrade_data_realtime(data: DataFrame) -> DataFrame:
    normalized = convert_timestamp_column(data, "t", "event_time")
    
    result = (
        normalized
        .select(
            col("s").alias("symbol"),
            col("p").alias("price"),
            col("event_time"),
            col("v").alias("volume"),
            col("c").alias("conditions")
        )
        .filter(
            (col("symbol").isNotNull()) & 
            (col("price").isNotNull()) &
            (col("event_time").isNotNull())
        )
    )
    
    return result