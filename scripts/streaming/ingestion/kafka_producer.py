from kafka import KafkaProducer
from config.settings import settings
from config.kafka_config import kafka_config
import json
import logging

logger = logging.getLogger(__name__)

producer = KafkaProducer(
    **kafka_config.PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def send_message(topic: str, message: dict):
    try:
        producer.send(topic, value=message)
        producer.flush()
        logger.debug("Sent message to %s: %s", topic, message)
    except Exception as e:
        logger.exception("Failed to send message to Kafka: %s", e)