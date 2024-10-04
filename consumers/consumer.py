"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.cimpl import OFFSET_BEGINNING, OFFSET_END
from tornado import gen

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"  # ,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
SCHEMA_REGISTRY_HOST_URL = "http://localhost:8081"


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
            self,
            topic_name_pattern,
            message_handler,
            is_avro=True,
            offset_earliest=False,
            sleep_secs=1.0,
            consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # and use the Host URL for Kafka and Schema Registry!
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avroconsumer-legacy
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#consumer
        # https://docs.confluent.io/platform/7.7/installation/configuration/consumer-configs.html#auto-offset-reset
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,  # from API spec
            # When using Avro with Kafka, the producer must define an Avro Schema for messages they produce
            "group.id": f"{self.topic_name_pattern}",
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_HOST_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # configure the AvroConsumer and subscribe to the topics and invoke on_assign callback
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#consumer
        self.consumer.subscribe(topics=[self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to the beginning or earliest
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-topicpartition
        for partition in partitions:
            # do no use "earliest", does not work
            # offset is a long value,
            # or use any of these constants: OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_INVALID
            partition.offset = OFFSET_BEGINNING if self.offset_earliest else OFFSET_END

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        # Poll Kafka for messages, handle any errors or exceptions
        # return 1 when a message is processed, and 0 when no message is retrieved.
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avroconsumer-legacy
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.poll
        try:
            message = self.consumer.poll(timeout=self.consume_timeout)
            if message is None:
                logger.debug("no message received by consumer")
                return 0
            elif message.error() is not None:
                logger.error(f"error from consumer {message.error()}")
                return 0
            else:
                # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message
                logger.info(f"consumed message {message.key()}: {message.value()}")
                self.message_handler(message)
                return 1
        except SerializerError as e:
            logger.error(f"Message deserialization failed for {message} : {e}")
            return 0

    def close(self):
        """Cleans up any open kafka consumers"""
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.close
        self.consumer.close()
        logger.info(f"Closing consumer for {self.topic_name_pattern}")
