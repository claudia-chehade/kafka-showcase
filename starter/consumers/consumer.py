"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URL = 'PLAINTEXT://localhost:9092'

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
        logger.info(f'CONSUMER: got a topic_name_pattern {str(topic_name_pattern)}')
       
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # Configure the broker properties
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL, 
            "group.id": "0",
            "default.topic.config": {
                    "auto.offset.reset": "earliest"
                }
        }

        # Create the Consumer, using the appropriate type.
        if is_avro is True:
            logger.info('avro consumer created')
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            logger.info('consumer created')
            self.consumer = Consumer(self.broker_properties)

        # Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        self.consumer.subscribe([self.topic_name_pattern], on_assign= self.on_assign)

    # When using subscribe you can expect on_assign and on_revoke to be called with each group rebalance 
    # or new subscription.
    # Group rebalances are triggered each time a consumer joins or leaves the group voluntarily or involuntarily. 
    # Involuntary group departures are triggered whenever your client fails to heartbeat back to the coordinator 
    # within the configured session.timeout.ms
    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING

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
        # Poll Kafka for messages. 
        try:
            record = self.consumer.poll(self.consume_timeout);
            if record is None:
                return 0
            elif record.error() is not None:
                logger.error(f"error while consuming: {message.error()}")
                return 0           
            else:
#                 logger.info(f"consumed {record.key()}-> {record.value()}")
                self.message_handler(record)
                return 1
        except SerializerError as err:
            logger.error(f"Error during serialization: {err.message}")
            return 0

    def close(self):
        """Cleans up any open kafka consumers"""
        if not self.consumer is None:
            self.consumer.close()