from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from config.kafka_config import kafka_config
from config.settings import settings
from processing.parser import parse_market_message
from processing.bigquery_sink import BigQuerySinkMapFunction

def kafka_to_bigquery_job():
    env = StreamExecutionEnvironment.get_execution_environment()

    # ⚠️ chỉ set default, cluster sẽ override nếu cần
    env.set_parallelism(4)

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
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource"
    )

    def transform(msg_str: str):
        try:
            msg = parse_market_message(msg_str)
            if msg.price and msg.price > 0:
                return msg.dict()
        except Exception:
            return None

    ds = (
        ds.map(transform, output_type=Types.PICKLED_BYTE_ARRAY())
          .filter(lambda x: x is not None)
    )

    if settings.ENABLE_BIGQUERY:
        ds.map(BigQuerySinkMapFunction())

    env.execute("kafka_to_bigquery_streaming_job")
