"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"
POSTGRES_HOST_URL = "jdbc:postgresql://postgres:5432/cta"  # from Readme


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # extract station information from PostgreSQL database into Kafka using Kafka JDBC Source Connector
    # see https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html
    # To delete a misconfigured connector: CURL -X DELETE localhost:8083/connectors/stations
    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": POSTGRES_HOST_URL,
               "connection.user": "cta_admin",  # readme
               "connection.password": "chicago",  # readme
               "table.whitelist": CONNECTOR_NAME,
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "org.chicago.cta.",
               "poll.interval.ms": 1000 * 60,  # every 60 seconds
           }
       }),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
