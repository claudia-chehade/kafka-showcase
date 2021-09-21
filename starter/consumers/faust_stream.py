"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)
IN_TOPIC ='stations'
OUT_TOPIC = 'org.chicago.cta.stations.table.v1'


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


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
#  Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic(IN_TOPIC, value_type=Station)
#  Define the output Kafka Topic
out_topic = app.topic(OUT_TOPIC, partitions=1)
# TODO: Define a Faust Table
transformed_station_table = app.Table(
   OUT_TOPIC,
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
@app.agent(IN_TOPIC)
async def station(stations):
    async for station in stations:
        transformed_station = transform(station)
        transformed_station_table[station.station_id] = transformed_station
        await out_topic.send(value=transformed_station)


def transform(station):
    transformed_station = TransformedStation()
    transformed_station.station_id = station.station_id
    transformed_station.station_name = station.station_name
    transformed_station.order = station.order
    transformed_station.line = get_line(station)
    return transformed_station

def get_line(station):
    if station.red:
        return 'red'
    if station.blue:
        return 'blue'
    if station.green:
        return 'green'
    return ''

if __name__ == "__main__":
    app.main()
