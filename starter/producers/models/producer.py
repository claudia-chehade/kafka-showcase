"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

BROKER_URL = 'PLAINTEXT://localhost:9092'
SCHEMA_REGISTRY_URL = "http://localhost:8081/"


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
       
        # Configure the broker properties
        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL
        }
        self.client = AdminClient(self.broker_properties)        

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=schema_registry
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info(f"PRODUCER: check creation of topic for {self.topic_name}")

        client = AdminClient(self.broker_properties)
        all_topics = client.list_topics().topics
        for topic in all_topics:
                print(topic)
        if client.list_topics(self.topic_name) is None:
            logger.info(f'Create topic {self.topic_name}')
            NewTopic(
                topic=self.topic_name,
                num_partitions= 2,
                replication_factor=1,
                config={
                'cleanup.policy':'delete',
                'compression.type':'lz4',
                'delete.retention.ms': 100
                }
            )

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.info("producer close")
        self.producer.flush()
        self.producer.close()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
