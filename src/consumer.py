from utils_ import spark_utils 
from utils_ import kafka_utils 
import json
import logging
from utils_.logging_config import *



def main():
    logger.info("Starting the streaming job...")
    # Load configurations
    logger.info("Loading Kafka and Spark configurations")
    try:
        with open('config/kafka_config.json') as f:
            kafka_config = json.load(f)
        logger.info("Kafka configuration loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading Kafka configuration: {e}")
        return

    try:
        with open('config/spark_config.json') as f:
            spark_config = json.load(f)
        logger.info("Spark configuration loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading Spark configuration: {e}")
        return

    try:
        with open('config/cassandra_config.json') as f:
            cassandra_config = json.load(f)
        logger.info("Cassandra configuration loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading Cassandra configuration: {e}")
        return
    def writeToCassandra(writeDf,epochId):
        with open('config/cassandra_config.json') as f:
            cassandra_config = json.load(f)
        writeDf.write.format("org.apache.spark.sql.cassandra")\
                    .mode("append")\
                    .option("spark.cassandra.connection.host",cassandra_config['cassandra_host'])\
                    .option("keyspace", cassandra_config['cassandra_keyspace'])\
                    .option("table", cassandra_config['cassandra_table'])\
                    .save()
    output_csv_dir = 'output'
    
    # Create Spark session
    logger.info("Creating Spark session")
    try:
        spark = spark_utils.create_spark_session('config/spark_config.json')
        logger.info("Spark session created successfully.")
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        return
    
    # Read stream from Kafka
    logger.info("Reading stream from Kafka...")
    try:
        df = spark_utils.read_stream_from_kafka(spark, 'config/kafka_config.json')
        logger.info("Stream data read from Kafka successfully.")
    except Exception as e:
        logger.error(f"Error reading stream from Kafka: {e}")
        return

    # Process the stream data
    logger.info("Processing the stream data")
    try:
        df_processed = spark_utils.process_stream_data(df)
        logger.info("Stream data processed successfully.")
    except Exception as e:
        logger.error(f"Error processing stream data: {e}")
        return
    logger.info("Writing processed data to Cassandra...")
    try:
        df_processed \
            .writeStream\
            .trigger(processingTime="10 seconds") \
            .outputMode("update")\
            .foreachBatch(writeToCassandra)\
            .option("checkpointLocation", spark_config["checkpoint_dir"]) \
            .start() \
            .awaitTermination()
        logger.info("Processed data successfully written to Cassandra.")
    except Exception as e:
        logger.error(f"Error writing processed data to Cassandra: {e}")
    # Write the processed data to Kafka
    # logger.info("Writing processed data to Kafka...")
    # try:
    #     df_processed \
    #         .selectExpr("to_json(struct(*)) AS value") \
    #         .writeStream \
    #         .trigger(processingTime='15 seconds') \
    #         .outputMode("update") \
    #         .format("kafka") \
    #         .option("kafka.bootstrap.servers", kafka_config["kafka_bootstrap_servers"]) \
    #         .option("topic", kafka_config["output_topic"]) \
    #         .option("checkpointLocation", spark_config["checkpoint_dir"]) \
    #         .start() \
    #         .awaitTermination()
    #     logger.info("Processed data successfully written to Kafka.")
    # except Exception as e:
    #     logger.error(f"Error writing processed data to Kafka: {e}")
    
    # Optionally write the processed data to CSV (if uncommented)
    #logger.info("Writing processed data to CSV...")
    # try:
    #     csv_query = df_processed \
    #         .writeStream \
    #         .trigger(processingTime='15 seconds') \
    #         .outputMode("append") \
    #         .format("csv") \
    #         .option("path", output_csv_dir) \
    #         .option("checkpointLocation", spark_config["checkpoint_dir"] + "/csv_checkpoint") \
    #         .start()
    #     csv_query.awaitTermination()
    #     logger.info("Processed data successfully written to CSV.")
    # except Exception as e:
    #     logger.error(f"Error writing processed data to CSV: {e}")

if __name__ == "__main__":
    main()
