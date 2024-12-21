import random
import json
from faker import Faker
from kafka import KafkaProducer
import time
from utils_.logging_config import *

fake = Faker()

def generate_fake_finance_data():
    data = {
        "transaction_id": fake.uuid4(),
        "account_number": fake.random_number(digits=10),
        "account_holder": fake.name(),
        "transaction_type": random.choice(['deposit', 'withdrawal', 'transfer']),
        "amount": round(random.uniform(1, 10000), 2),
        "currency": random.choice(['USD', 'EUR', 'GBP']),
        "transaction_date": str(fake.date_this_year()),
        "merchant_name": fake.company(),
        "merchant_category": random.choice(['Retail', 'Food', 'Entertainment']),
        "location": fake.city(),
        "balance_after_transaction": round(random.uniform(1000, 50000), 2),
        # Nested list of items in the transaction (e.g., products purchased)
        "transaction_items": [
            {
                "item_id": fake.uuid4(),
                "product_name": fake.word(),
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(5, 500), 2),
                "total_price": round(random.uniform(5, 500), 2) * random.randint(1, 5),
            }
            for _ in range(random.randint(1, 3))  # Random number of items (1-3)
        ],

        # Nested object for shipping information
        "shipping_info": {
            "address": fake.address(),
            "shipping_method": random.choice(['Standard', 'Express', 'Overnight']),
            "shipping_cost": round(random.uniform(5, 100), 2),
            "estimated_delivery": str(fake.date_this_month()),
            "tracking_number": fake.uuid4(),
        },

        # Additional optional fields
        "customer_feedback": random.choice([True, False]),
        "discount_applied": random.choice([True, False]),
    
    }
    return data

def send_fake_data_to_kafka(producer, topic, count=10):
    
    for _ in range(count):
        try:
            # Produce the message to Kafka
            data = generate_fake_finance_data()
            producer.send(topic, value=data)
            logger.info(f"Sent to Kafka")
            print(f"Sent to Kafka: {json.dumps(data, indent=2)}")  # Pretty print for logging
            time.sleep(5)
        except Exception as e:
            print(f"Error producing message: {e}")
            
