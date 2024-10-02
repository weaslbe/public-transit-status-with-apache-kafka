"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Using Faust Stream Processing to transform the raw Stations table that we ingested from Kafka Connect.
# The raw format from the database has more data than we need, and the line color information is not conveniently
# configured. To fix this, we're going to ingest data from our Kafka Connect topic and transform it

# run with: faust -A faust_stream worker -l inf

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# a Faust Stream that ingests data from the Kafka Connect stations topic and places it into a new topic with only the
# necessary information.
# https://faust.readthedocs.io/en/latest/userguide/application.html#application-parameters
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# the input Kafka Topic, the topic Kafka Connect did output to in connectors
# https://faust.readthedocs.io/en/latest/userguide/application.html#app-topic-create-a-topic-description
topic = app.topic("org.chicago.cta.stations", value_type=Station)

faust_stations_table_transformed_name = "org.chicago.cta.stations.table.v1"
# the output Kafka Topic
out_topic = app.topic(faust_stations_table_transformed_name, partitions=1)

# a Faust Table
# https://faust.readthedocs.io/en/latest/reference/faust.tables.table.html?highlight=hopping#faust.tables.table.Table
table = app.Table(
   name=faust_stations_table_transformed_name,
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)

# Using Faust, transforming input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`

# Create an async function to be called when an event is received from our Kafka topic, with "app.agent" decorater,
# which tells Faust to route events from the specified topic to this function and registers the function as a callback
# for the application when data is received
# The inner for loop is iterating on an infinite stream of relevant events coming through the stream
# async keyword for asyncio, keyword is mandatory on function and for loop.
# https://faust.readthedocs.io/en/latest/userguide/application.html#app-agent-define-a-new-stream-processor
# https://faust.readthedocs.io/en/latest/userguide/agents.html#the-stream


@app.agent(topic)
async def process(stations):
    async for station in stations:
        # Transform the station data by setting the line to the correct color, see consumers/models/line.py
        line = None
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        elif station.green:
            line = "green"
        else:
            logger.warning(f"Could not find line color for station {station.station_id} with station being {station}")

        # Produce the transformed data to the out_topic
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )
        # or via
        # await out_topic.send(key=station.station_id, value=TransformedStation(...))

if __name__ == "__main__":
    app.main()
