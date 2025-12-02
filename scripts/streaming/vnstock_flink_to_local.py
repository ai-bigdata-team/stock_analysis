import os
import json


from pyflink.common import WatermarkStrategy, Encoder
from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.formats.parquet import ParquetWriterFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Row, RowType, RowField, DataTypes
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, RollingPolicy, OutputFileConfig
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.formats.parquet import ParquetWriterFactory

from dotenv import load_dotenv

load_dotenv()

GCS_BUCKET = "stock-data"
RAW_PATH = f"gs://{GCS_BUCKET}/stream_data/raw/trades"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/stream_data/checkpoints/raw_trades"

# Define row_type schema for Parquet
row_type = RowType([
    RowField("index", DataTypes.INT()),
    RowField("time", DataTypes.STRING()),
    RowField("open", DataTypes.DOUBLE()),
    RowField("high", DataTypes.DOUBLE()),
    RowField("low", DataTypes.DOUBLE()),
    RowField("close", DataTypes.DOUBLE()),
    RowField("volume", DataTypes.INT()),
    RowField("ticker", DataTypes.STRING()),
    RowField("difference", DataTypes.DOUBLE())
])


def to_partition_path(row: Row):
    symbol = row.ticker
    return f"gs://{GCS_BUCKET}/stock_data/raw/trades/symbol={symbol}"

def build_parquet_sink(base_path, parquet_factory):
    return (
        FileSink.for_bulk_format(base_path, parquet_factory)
        .with_bucket_assigner(lambda row: f"symbol={row.ticker}")
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )


def kafka_sink():
    """A Flink task which sinks a kafka topic to a file on disk

    We will read from a kafka topic and then perform a count() on
    the message to count the number of characters in the message.
    We will then save that count to the file on disk.
    """
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///home/pavt1024/bigdata/stock_analysis/jars/flink/flink-connector-kafka-4.0.1-2.0.jar", 
                 "file:///home/pavt1024/bigdata/stock_analysis/jars/kafka-clients-4.0.1.jar",
                 "file:///home/pavt1024/bigdata/stock_analysis/jars/gcs/gcs-connector-shaded.jar")

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BROKER"])
        .set_topics(os.environ["KAFKA_TOPIC"])
        .set_group_id("flink_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Adding our kafka source to our environment
    ds = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source"
    )

    # Just count the length of the string. You could get way more complex
    # here
    def demo_function(sample_str):
        sample = json.loads(sample_str)
        difference = sample['high'] - sample['low']
        # Trả về Row, đúng với ROW_NAMED
        return Row(
            sample['index'],
            sample['time'],
            sample['open'],
            sample['high'],
            sample['low'],
            sample['close'],
            sample['volume'],
            sample['ticker'],
            difference
        )
    output_type = Types.ROW_NAMED(
        ["index", "time", "open", "high", "low", "close", "volume", "ticker", "difference"],
        [Types.INT(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
        Types.DOUBLE(), Types.INT(), Types.STRING(), Types.DOUBLE()]
    )

    ds = ds.map(demo_function, output_type=output_type)
    parquet_factory = ParquetWriterFactory.for_row_type(row_type)

    sink = build_parquet_sink(RAW_PATH, parquet_factory)
    ds.sink_to(sink)

    env.execute("kafka_to_gcs_sink")



if __name__ == "__main__":
    kafka_sink()
