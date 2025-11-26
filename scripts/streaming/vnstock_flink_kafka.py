import os
import json


from pyflink.common import WatermarkStrategy, Encoder
from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.formats.parquet import ParquetWriterFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, RollingPolicy, OutputFileConfig
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from dotenv import load_dotenv

load_dotenv()

def kafka_sink_example():
    """A Flink task which sinks a kafka topic to a file on disk

    We will read from a kafka topic and then perform a count() on
    the message to count the number of characters in the message.
    We will then save that count to the file on disk.
    """
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    # the kafka/sql jar is used here as it's a fat jar and could avoid dependency issues
    env.add_jars("file:///home/pavt1024/bigdata/stock_analysis/jars/flink/flink-connector-kafka-4.0.1-2.0.jar", 
                 "file:///home/pavt1024/bigdata/stock_analysis/jars/kafka-clients-4.0.1.jar")
    # or 
    # env.add_jars("file:///home/pavt1024/bigdata/stock_analysis/jars/flink/flink-sql-connector-kafka-4.0.1-2.0.jar")

    # Define the new kafka source with our docker brokers/topics
    # This creates a source which will listen to our kafka broker
    # on the topic we created. It will read from the earliest offset
    # and just use a simple string schema for serialization (no JSON/Proto/etc)
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

    output_path = os.path.join(os.environ.get("KAFKA_SINK_DIR", "/sink"), "sink.log")

    # This is the sink that we will write to
    sink = (
        FileSink.for_row_format(
            base_path=output_path, encoder=Encoder.simple_string_encoder()
        )
        .with_output_file_config(OutputFileConfig.builder().build())
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )

    # Writing the processed stream to the file
    ds.sink_to(sink=sink)

    # Execute the job and submit the job
    env.execute("kafka_sink_example")


if __name__ == "__main__":
    kafka_sink_example()
