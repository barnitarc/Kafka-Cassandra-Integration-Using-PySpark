from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, window, to_timestamp, from_json, lit, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
import json
from utils_.logging_config import *



def create_spark_session(config_path):
    """
    Create and return a Spark session with optimized configurations.
    """
    with open(config_path) as f:
        config = json.load(f)

    spark = SparkSession.builder \
        .appName(config["spark_config"]["spark.app.name"]) \
        .master(config["spark_config"]["spark.master"]) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
        .config("spark.sql.shuffle.partitions", config["spark_config"]["spark.sql.shuffle.partitions"]) \
        .config("spark.streaming.backpressure.enabled", config["spark_config"]["spark.streaming.backpressure.enabled"]) \
        .config("spark.streaming.backpressure.initialRate", config["spark_config"]["spark.streaming.backpressure.initialRate"]) \
        .config("spark.streaming.kafka.maxRatePerPartition", config["spark_config"]["spark.streaming.kafka.maxRatePerPartition"]) \
        .config("spark.executor.memory", config["spark_config"]["spark.executor.memory"]) \
        .config("spark.driver.memory", config["spark_config"]["spark.driver.memory"]) \
        .getOrCreate()
    
    logger.info(f"Spark session created with app name: {config['spark_config']['spark.app.name']}")
    return spark

def read_stream_from_kafka(spark, config_path):
    """
    Read data from Kafka and return as a Spark DataFrame.
    """
    with open(config_path) as f:
        config = json.load(f)
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("failOnDataLoss","false")\
        .option("subscribe","incoming_finance_data") \
        .option("startingOffsets", "latest") \
        .load()
        
    
    return df

def process_stream_data(df):
    """
    Example function to perform some transformations on the stream data.
    """
    # Example transformation: Extract 'value' and process it as a JSON object
    from pyspark.sql.functions import from_json
    transaction_item_schema = StructType([
        StructField("item_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", FloatType(), True),
        StructField("total_price", FloatType(), True),
    ])

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_number", StringType(), True),
        StructField("account_holder", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("location", StringType(), True),
        StructField("balance_after_transaction", FloatType(), True),
        StructField("transaction_items", ArrayType(transaction_item_schema), True),
        StructField("shipping_info", StructType([
            StructField("address", StringType(), True),
            StructField("shipping_method", StringType(), True),
            StructField("shipping_cost", FloatType(), True),
            StructField("estimated_delivery", StringType(), True),
            StructField("tracking_number", StringType(), True)
        ]), True),
        StructField("customer_feedback", BooleanType(), True),
        StructField("discount_applied", BooleanType(), True)
    ])
    
    df_json = df.select(from_json(col("value").cast("string"), schema).alias("data"))
    
    # Extract the nested fields from the JSON
    df_exploded = df_json.select(
        "data.transaction_id",
        "data.account_number",
        "data.account_holder",
        "data.transaction_type",
        "data.amount",
        "data.currency",
        "data.transaction_date",
        "data.merchant_name",
        "data.merchant_category",
        "data.location",
        "data.balance_after_transaction",
        explode("data.transaction_items").alias("transaction_item"),
        "data.shipping_info",
        "data.customer_feedback",
        "data.discount_applied"
    )

    # Flatten the transaction item structure
    df_flat = df_exploded.select(
        "transaction_id",
        "account_number",
        "account_holder",
        "transaction_type",
        "amount",
        "currency",
        "transaction_date",
        "merchant_name",
        "merchant_category",
        "location",
        "balance_after_transaction",
        "transaction_item.item_id",
        "transaction_item.product_name",
        "transaction_item.quantity",
        "transaction_item.unit_price",
        "transaction_item.total_price",
        "shipping_info.address",
        "shipping_info.shipping_method",
        "shipping_info.shipping_cost",
        "shipping_info.estimated_delivery",
        "shipping_info.tracking_number",
        "customer_feedback",
        "discount_applied"
    )
        # Perform some data transformations (Example: Clean up null values and apply transformations)
    df_transformed = df_flat \
        .withColumn("amount", col("amount").cast(FloatType())) \
        .withColumn("transaction_date", to_timestamp(col("transaction_date"), "yyyy-MM-dd")) \
        .fillna({"amount": 0, "balance_after_transaction": 0}) \
        .filter(col("transaction_type").isNotNull())  # Filter out rows with null transaction_type

    # Wont work in streaming- Window function example: Calculate running total of transaction amount per account
    # window_spec = Window \
    #     .partitionBy("account_number") \
    #     .orderBy("transaction_date")

    # df_with_running_total = df_transformed \
    #     .withColumn("running_total", sum("amount").over(window_spec))

    df_transformed=df_transformed.dropDuplicates(["merchant_name", "transaction_date","amount","transaction_id"])
    df_grouped_by_merchant = df_transformed \
        .groupBy("merchant_category") \
        .agg(
            sum("amount").alias("total_amount"),
            count("transaction_id").alias("transaction_count")
        )
    
    # Grouping example: Calculate total transaction amount by merchant
    logger.info("Write the processed data back to another Kafka topic")
    
    
    df_with_window = df_transformed \
        .withWatermark("transaction_date", "1 day")\
        .groupBy(
            window(col("transaction_date"), "1 day"),  # 1-minute window based on transaction_date
            "account_number"
        ) \
        .agg(
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.count("transaction_id").alias("transaction_count")
        )

    # Flatten the window structure (window starts and ends)
    df_with_window = df_with_window.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "account_number",
        "total_amount",
        "avg_amount",
        "transaction_count"
    )
    df=df_with_window
    
    return df_grouped_by_merchant
