from __future__ import absolute_import
from utils_ import kafka_utils
from utils_ import data_generator
from utils_.logging_config import *
import json

def main():
    with open('config/kafka_config.json') as f:
        kafka_config = json.load(f)
    logger.info("Start producing data")
    producer = kafka_utils.create_kafka_producer(kafka_config["kafka_bootstrap_servers"])
    data_generator.send_fake_data_to_kafka(producer, kafka_config["input_topic"], count=10)

if __name__ == "__main__":
    main()