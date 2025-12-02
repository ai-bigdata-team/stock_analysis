import os
import json
from typing import List

from google.cloud import bigquery
from pyflink.common import WatermarkStrategy, Encoder
from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.formats.parquet import ParquetWriterFactory
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, RollingPolicy, OutputFileConfig
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import RuntimeContext, SinkFunction

from dotenv import load_dotenv

load_dotenv()

class BigQuerySink(SinkFunction):

    def __init__(self, project_id: str, dataset_id: str, table_id: str, batch_size: int = 500):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.batch_size = batch_size
        self.client = None
        self.table_ref = None
        self.buffer: List[dict] = []

    def open(self, runtime_context: RuntimeContext):
        self.client = bigquery.Client(project=self.project_id)
        self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)

    def invoke(self, value, context):
        row = {
            "conditions": value[0],
            "price": value[1],
            "symbol": value[2],
            "timestamp": value[3],
            "volume": value[4],
        }
        self.buffer.append(row)
        if len(self.buffer) >= self.batch_size:
            self._flush()

    def close(self):
        if self.buffer:
            self._flush()

    def _flush(self):
        errors = self.client.insert_rows_json(self.table_ref, self.buffer)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        self.buffer.clear()

def kafka_sink():
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
        .set_topics("finnhub_stock")
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
    def parse_trade(sample_str):
        sample = json.loads(sample_str)
        return Row(
            ",".join(sample.get("c", [])),   # conditions
            sample.get("p", 0.0),            # price
            sample.get("s", ""),             # symbol
            sample.get("t", 0),              # timestamp
            sample.get("v", 0.0)             # volume
        )
    output_type = Types.ROW_NAMED(
        ["conditions", "price", "symbol", "timestamp", "volume"],
        [Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.LONG(), Types.DOUBLE()]
    )

    ds = ds.map(parse_trade, output_type=output_type)

    bq_project = os.environ.get("GCP_PROJECT_ID", "bigdata-project")
    bq_dataset = os.environ.get("BQ_DATASET", "stock_data")
    bq_table = os.environ.get("BQ_TABLE", "finnhub_trades")

    ds.add_sink(BigQuerySink(bq_project, bq_dataset, bq_table))

    # Execute the job and submit the job
    env.execute("kafka_sink")


if __name__ == "__main__":
    kafka_sink()
