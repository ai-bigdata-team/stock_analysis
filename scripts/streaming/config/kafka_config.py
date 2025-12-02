from .settings import settings

class KafkaConfig:

    PRODUCER_CONFIG = {
        "bootstrap_servers": settings.KAFKA_BROKER,
        "acks": "all",
        "retries": 3,
        "compression_type": "snappy",
        "max_block_ms": 5000,
    }

    CONSUMER_CONFIG = {
        "bootstrap_servers": settings.KAFKA_BROKER,
        "group_id": "market-data-consumer",
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
    }

    TOPICS = {
        "market_data": settings.KAFKA_TOPIC,
        "errors": f"{settings.KAFKA_TOPIC}-errors",
    }

    ADMIN_CONFIG = {
        "bootstrap_servers": settings.KAFKA_BROKER,
        "client_id": "admin",
    }



kafka_config = KafkaConfig()
