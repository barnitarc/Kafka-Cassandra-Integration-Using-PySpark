from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import random
import faker
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime
# Create a Spark session

def writeToCassandra(writeDf,epochId):
    writeDf.write.format("org.apache.spark.sql.cassandra")\
                .mode("append")\
                .option("keyspace", "test").option("table", "words")\
                .save()

spark = SparkSession.builder \
    .appName("CassandraDataLoader") \
    .config("spark.cassandra.connection.host", "127.0.0.1")\
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0")\
    .getOrCreate()

spark_df=spark.readStream\
                .format("socket")\
                .option("host","localhost")\
                .option("port","1100")\
                .load()
words_df=spark_df.select(explode(split('value',' ')).alias('word'))
# words_df=words_df.withColumn("timestamp")
word_count_df=words_df.groupBy("word").count()
# Generate dummy data using the Faker library
fake = faker.Faker()

# Create a list of dummy data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("phone", StringType(), True)
])

# Create some dummy data
data = [
    (1, "alice.johnson@example.com", "Alice Johnson", "567890"),
    (2, "bob.smith@example.com", "Bob Smith", "561981"),
    (3, "charlie.davis@example.com", "Charlie Davis", "334455")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)
# Write the DataFrame to the Cassandra users table
# word_count_df.writeStream \
#     .format("org.apache.spark.sql.cassandra").outputMode("update") \
#     .option("checkpointLocation","/tmp/checkpoint_/")\
#     .option("keyspace", "test").option("table", "words")\
#     .start().awaitTermination()

word_count_df.writeStream\
            .outputMode("update")\
            .foreachBatch(writeToCassandra)\
            .start().awaitTermination()

# Verify by reading the data back from Cassandra (optional)
# result_df = spark.read \
#    .format("org.apache.spark.sql.cassandra")\
# .option("keyspace", "test")\
# .option("table", "words").load()

# # Show the loaded data
# result_df.show(10)  # Show the first 10 records

# # Stop the Spark session
# spark.stop()
