"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from .producer import Producer
from .turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)

turnstile_topic_name = "org.chicago.cta.turnstile.v1"


class Turnstile(Producer):
    # see https://avro.apache.org/docs/1.8.2/spec.html
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        super().__init__(
            turnstile_topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avroproducer-legacy
        # emit a message to the turnstile topic for the number of entries that were calculated
        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color.name,

                },
                key_schema=self.key_schema,
                value_schema=self.value_schema,
            )
