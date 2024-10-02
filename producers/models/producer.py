"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

# as is previous assignments, from Kafka row in readme
#BROKER_URL = "localhost:9092"
BROKER_URL = "PLAINTEXT://localhost:9092"  #,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
SCHEMA_REGISTRY_HOST_URL = "http://localhost:8081"  # from readme, as per comment


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,  # from API spec
            # When using Avro with Kafka, the producer must define an Avro Schema for messages they produce
            "schema.registry.url": SCHEMA_REGISTRY_HOST_URL
        }
        # self.admin_client = AdminClient({"bootstrap.servers": BROKER_URL})

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # parameters from https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/
        # index.html#confluent_kafka.avro.AvroProducer
        # or
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avroproducer-legacy
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def topic_exists(self, client: AdminClient, topic_name):
        """Checks if the given topic exists in the Kafka Broker"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # as from similar exercises, use AdminClient to check for and create topic
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        topic_exists = self.topic_exists(client, self.topic_name)
        logger.info(f"Does Topic {self.topic_name} exist: {topic_exists}")

        if topic_exists:
            return

        if not topic_exists:
            # create topic
            futures = client.create_topics(
                [NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas
                )]
            )

            for current_topic_name, future in futures.items():
                try:
                    future.result()
                    logger.info(f"topic {current_topic_name} created")
                except Exception as e:
                    logger.error(f"failed to create topic {current_topic_name}: {e}")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer:
            # https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
            self.producer.flush(timeout=5)
