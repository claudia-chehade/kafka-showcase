import asyncio
from dataclasses import asdict, dataclass, field
import json
import random
import time
import logging

from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from producer import ProducerAvro


SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"

logger = logging.getLogger(__name__)    

def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        test_avro = TestAvro(123, 'ohare', None, 'a', 'b')
        test_avro.produce("BL000", "b", 1, "b")
    except KeyboardInterrupt as e:
        print("shutting down")

class TestAvro(ProducerAvro):  

    def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        self.name = name
        station_name = (
            self.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # self.schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        topic_name = f"arrival_{station_name}_testavro1" 
        value_schema_string = """{
        "doc": "Arrival values.",
        "name": "arrival.value",
        "namespace": "com.udacity",
        "type": "record",
        "fields": [
            {"name": "station_id", "type": "int"},
            {"name": "train_id", "type":  "string"},
            {"name": "direction", "type":  "string"}      
        ]
        }"""
        # value_schema_string = value_schema_string.replace("com.udacity", topic_name)
        logger.info(f'STATION schema: {value_schema_string}')
        self.value_schema = avro.loads(value_schema_string)

        key_schema_string = """{
            "namespace": "com.udacity",
            "type": "record",
            "name": "arrival.key",
            "fields": [
                {
                "name": "timestamp",
                "type": "long"
                }
            ]
            }"""
        self.key_schema = avro.loads(key_schema_string)

        super().__init__(
            topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema, 
            num_partitions=1,
            num_replicas=1,
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
       
        logger.info(f"TEST_AVRO:  {type(self.station_id)}" )

   
    def produce(self, train, direction, prev_station_id, prev_direction):
        timestamp = self.time_millis()
               
        logger.info(f"STATION: arrival produce:  {train} at {timestamp} from {prev_station_id}/{prev_direction} to {direction}" )
        value={
               "station_id": 40890,
               "train_id": "BL000",
               "direction": "b"
            #    "station_id": self.station_id,
            #    "train_id": train.train_id,
            #    "direction": direction #,
            #    'prev_station_id': prev_station_id,
            #    'prev_direction': prev_direction
           }
        # p = self.producer
        p = AvroProducer({"bootstrap.servers": BROKER_URL}, schema_registry=CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL}))
        p.produce(topic=self.topic_name, 
            key={"timestamp": timestamp}, 
            value=value, 
            value_schema=self.value_schema, 
            key_schema = self.key_schema)

   
    def time_millis(self):
        return int(round(time.time() * 1000))

if __name__ == "__main__":
    main()
