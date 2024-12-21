# Kafka-Cassandra Integration Using PySpark

This project demonstrates how to integrate Apache Kafka and Apache Cassandra using PySpark for real-time data processing and storage in Ubuntu 20.04.

## Table of Contents

- [Introduction](#introduction)
- [Architecture](#architecture)
- [Process Flow and Transformations](#process-flow-and-transformations)
- [Setup and Installation](#setup-and-installation)
- [Configuration](#configuration)
- [Running the Application](#running-the-application)
- [Cassandra Table Schema](#cassandra-table-schema)
- [Contributing](#contributing)

## Introduction

In modern data architectures, integrating streaming platforms like Apache Kafka with scalable databases such as Apache Cassandra is essential for building real-time analytics and event-driven applications. This project provides a practical example of consuming data from Kafka, processing it with PySpark, and storing the results in Cassandra.

## Architecture

The data pipeline consists of the following components:

1. **Kafka Producer**: Simulates real-time data by sending messages to a Kafka topic.
2. **PySpark Structured Streaming**: Consumes messages from the Kafka topic, processes the data, and performs necessary transformations.
3. **Cassandra Database**: Stores the processed data for further analysis and querying.

## Process Flow and Transformations

The following steps outline the process flow and transformations applied to the data:

1. **Data Ingestion**:
   - Kafka producer generates transaction data in JSON format and publishes it to a Kafka topic.

2. **Data Consumption**:
   - PySpark Structured Streaming reads data from the Kafka topic as a streaming source.

3. **Transformations**:
   - **Flattening Nested Structures**: Explodes and flattens nested fields such as `transaction_items` and `shipping_info` into individual columns.
   - **Aggregations**:
     - Collects all product names for each `account_number` as a comma-separated list (`products_list`).
     - Calculates the total price spent (`total_price_spent`) by summing up `quantity * unit_price` for each account.
     - Computes the total quantity of items bought (`total_quantity_bought`) for each account.
   - **Data Enrichment**:
     - Adds derived columns to provide richer insights, such as calculated totals.

4. **Data Storage**:
   - The processed data is written to a Cassandra table with a schema optimized for querying.

## Setup and Installation

Follow these steps to set up the project:

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/barnitarc/Kafka-Cassandra-Integration-Using-PySpark.git
   cd Kafka-Cassandra-Integration-Using-PySpark
   ```

2. **Install Dependencies**:

   Ensure you have Python installed. Then, install the required Python packages:

   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up Apache Kafka**:

   - Download and install [Apache Kafka](https://kafka.apache.org/downloads). I have followed [this](https://drive.google.com/file/d/1jXW3kd7ZSBobJjEAQRSzNo0Nma6YLeyz/view?usp=drive_link) for installing Kafka in Ubuntu.
   - Start the ZooKeeper service:

     ```bash
     sudo systemctl start zookeeper
     ```

   - Start the Kafka broker:

     ```bash
     sudo systemctl star kafka
     ```
   - Create the Kafka Topic:

     ```bash
     kafka-topics.sh --create --topic incoming_finance_data --bootstrap-server localhost:9092
     ```

4. **Set Up Apache Cassandra**:

   - Download and install [Apache Cassandra](https://cassandra.apache.org/download/). I have followed [this](https://drive.google.com/file/d/1CqFuvWItUvGGVPGLuVI9uycfIpBm6vWT/view?usp=drive_link) for installing Cassandra in Ubuntu.
   - Start the Cassandra server:

     ```bash
     sudo systemctl start cassandra
     ```
   - Check the status of the Cassandra server:

     ```bash
     sudo systemctl status cassandra
     ```
   - Check if you can access cqlsh:

     ```bash
     ~/.local/bin/cqlsh
        or
     cqlsh
     ```

## Configuration

Configure the application by modifying the `config/*_config.json` files. This file contains settings for Kafka topics, Cassandra keyspace, and other parameters.

## Running the Application

1. **Start the Kafka Producer**:

   The producer script sends simulated data to the specified Kafka topic. Currently this file produces 10 records every 5 seconds. We can either change the count of recrds or run the below commands multiple times to send data to kafka.

   ```bash
   python3 src/producer.py
   ```

2. **Start the PySpark Structured Streaming Application**:

   This script consumes data from Kafka, processes it, and writes the results to Cassandra.

   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/consumer.py
   ```

   Ensure that the package versions match your Spark and Scala versions.

## Cassandra Table Schema

Before running the application, create the necessary table in Cassandra:

```sql
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY,
    account_number TEXT,
    account_holder TEXT,
    transaction_type TEXT,
    amount DOUBLE,
    currency TEXT,
    transaction_date DATE,
    merchant_name TEXT,
    merchant_category TEXT,
    location TEXT,
    balance_after_transaction DOUBLE,
    shipping_address TEXT,
    shipping_method TEXT,
    shipping_cost DOUBLE,
    estimated_delivery DATE,
    tracking_number UUID,
    customer_feedback BOOLEAN,
    discount_applied BOOLEAN,
    products_list TEXT,
    total_price_spent DOUBLE,
    total_quantity_bought INT
);
```

This schema is designed to store transaction data processed by the PySpark application.
## Check Cassandra Table data

After running the application, check if the data is getting loaded in cqlsh table.
<!--![image](https://github.com/user-attachments/assets/69572182-fc7e-475d-b93f-53f4d5dc0362)-->

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes. Ensure that your code adheres to the project's coding standards and includes appropriate tests.

---

---

Feel free to explore, modify, and enhance this project to suit your needs. If you encounter any issues or have questions, please open an issue in the repository.
