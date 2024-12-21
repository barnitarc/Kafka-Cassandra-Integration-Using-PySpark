from kafka import KafkaProducer, KafkaConsumer
import json
import logging
from utils_.logging_config import *

logger = logging.getLogger(__name__)

def create_kafka_producer(bootstrap_servers):
    """
    Create a Kafka producer instance.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

# def create_kafka_consumer(bootstrap_servers, topic, group_id):
    # """
    # Create a Kafka consumer to read data from the topic.
    # """
    # consumer = KafkaConsumer(
    #     topic,
    #     group_id=group_id,
    #     bootstrap_servers=bootstrap_servers,
    #     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    # )
    # return consumer

def send_to_kafka(producer, topic, message):
    """
    Send data to Kafka topic.
    """
    try:
        producer.send(topic, value=message)
        logger.info(f"Sent message: {message}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")