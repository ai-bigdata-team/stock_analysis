from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from kafka import KafkaConsumer as PyKafkaConsumer
from config.settings import settings
from config.kafka_config import kafka_config
from .parser import parse_market_message
from .bigquery_sink import BigQuerySink, BigQuerySinkMapFunction
import os

def get_kafka_partition_count(topic: str, bootstrap_servers: str) -> int:
    consumer = PyKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="partition_check",
        enable_auto_commit=False
    )
    partitions = consumer.partitions_for_topic(topic)
    consumer.close()
    return len(partitions) if partitions else 4

def kafka_to_bigquery_job():
    # --- Stream Execution Environment ---
    env = StreamExecutionEnvironment.get_execution_environment()

    # --- Load Kafka connector JARs tuyệt đối ---
# --- Project root, tương đối từ file hiện tại ---
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

    # --- Thư mục chứa JAR ---
    JAR_DIR = os.path.join(PROJECT_ROOT, "jars")

    env.add_jars(
        f"file://{os.path.join(JAR_DIR, 'flink-connector-kafka-4.0.1-2.0.jar')}",
        f"file://{os.path.join(JAR_DIR, 'kafka-clients-4.0.1.jar')}"
    )

    # --- Parallelism ---
    partition_count = get_kafka_partition_count(
        kafka_config.TOPICS["market_data"],
        kafka_config.ADMIN_CONFIG["bootstrap_servers"]
    )
    env.set_parallelism(partition_count)

    # --- Kafka source ---
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_config.ADMIN_CONFIG["bootstrap_servers"])
        .set_topics(kafka_config.TOPICS["market_data"])
        .set_group_id(kafka_config.CONSUMER_CONFIG["group_id"])
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    # --- Transform + validate ---
    def transform(msg_str: str):
        try:
            msg = parse_market_message(msg_str)
            if msg.price is not None and msg.price > 0:
                return msg.dict()
        except Exception as e:
            print("Parse/Validation error:", e)
        return None

    ds_transformed = ds.map(transform, output_type=Types.PICKLED_BYTE_ARRAY()) \
                       .filter(lambda x: x is not None)

    # --- Sink BigQuery ---
    if settings.ENABLE_BIGQUERY:
        bq_sink = BigQuerySink()
        ds_transformed.map(BigQuerySinkMapFunction())

    # --- Execute job ---
    env.execute("Kafka -> Flink -> BigQuery")
