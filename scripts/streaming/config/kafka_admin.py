import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config.kafka_config import kafka_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_topic_if_not_exists(topic_name: str, num_partitions: int = 3, replication_factor: int = 1):
    """
    Kiểm tra topic, nếu chưa tồn tại thì tạo mới với partition/replication mong muốn.
    Sử dụng BROKER từ KafkaConfig.
    """
    admin = KafkaAdminClient(**kafka_config.ADMIN_CONFIG)
    try:
        topic_list = [NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )]
        admin.create_topics(new_topics=topic_list, validate_only=False)
        logger.info("Created topic '%s' with %d partitions, replication factor %d",
                    topic_name, num_partitions, replication_factor)
    except TopicAlreadyExistsError:
        logger.info("Topic '%s' already exists", topic_name)
    except Exception as e:
        logger.exception("Failed to create topic '%s': %s", topic_name, e)
    finally:
        admin.close()


def get_topic_partition_count(topic_name: str) -> int:
    """
    Lấy số partition của topic hiện tại.
    Trả về 1 nếu có lỗi.
    """
    admin = KafkaAdminClient(**kafka_config.ADMIN_CONFIG)
    try:
        metadata = admin.describe_topics([topic_name])
        if metadata and topic_name in metadata:
            partitions = metadata[topic_name].partitions
            count = len(partitions)
            logger.info("Topic '%s' has %d partitions", topic_name, count)
            return count
        logger.warning("No metadata found for topic '%s', fallback to 1 partition", topic_name)
        return 1
    except Exception as e:
        logger.warning("Failed to get partition count for '%s': %s", topic_name, e)
        return 1
    finally:
        admin.close()
