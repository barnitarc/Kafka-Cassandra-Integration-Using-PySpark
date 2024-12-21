from pyspark.sql.functions import from_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, window, to_timestamp, from_json, lit, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
import json
from faker import Faker
import random
from pyspark.sql.functions import to_json, col
spark = SparkSession.builder \
    .appName("FakeDataDirect") \
    .getOrCreate()
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
fake = Faker()
data = [{
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
for i in range(10)]
df_spark = spark.createDataFrame(data,schema).alias("data")

df_exploded = df_spark.select(
    col("data.transaction_id"),
    col("data.account_number"),
    col("account_holder"),
    col("transaction_type"),
    col("amount"),
    col("currency"),
    col("transaction_date"),
    col("merchant_name"),
    col("merchant_category"),
    col("location"),
    col("balance_after_transaction"),
    explode(col("transaction_items")).alias("transaction_item"),
    col("shipping_info"),
    col("customer_feedback"),
    col("discount_applied")
)

# Flatten the exploded DataFrame
df_flat = df_exploded.select(
    col("transaction_id"),
    col("account_number"),
    col("account_holder"),
    col("transaction_type"),
    col("amount"),
    col("currency"),
    col("transaction_date"),
    col("merchant_name"),
    col("merchant_category"),
    col("location"),
    col("balance_after_transaction"),
    col("transaction_item.item_id").alias("item_id"),
    col("transaction_item.product_name").alias("product_name"),
    col("transaction_item.quantity").alias("quantity"),
    col("transaction_item.unit_price").alias("unit_price"),
    col("transaction_item.total_price").alias("total_price"),
    col("shipping_info.address").alias("shipping_address"),
    col("shipping_info.shipping_method").alias("shipping_method"),
    col("shipping_info.shipping_cost").alias("shipping_cost"),
    col("shipping_info.estimated_delivery").alias("estimated_delivery"),
    col("shipping_info.tracking_number").alias("tracking_number"),
    col("customer_feedback"),
    col("discount_applied")
)
# Show the DataFrame
df_spark.show(truncate=False)



# Write the DataFrame to CSV
df_flat.coalesce(1).write.csv(
    path="D:\SparkProjects\TestData\data",
    mode="overwrite", # Overwrite existing files if they exist
    header=True        # Include the header row in the CSV
)

df_json = df_spark.select(
    col("transaction_id"),
    col("account_number"),
    col("account_holder"),
    col("transaction_type"),
    col("amount"),
    col("currency"),
    col("transaction_date"),
    col("merchant_name"),
    col("merchant_category"),
    col("location"),
    col("balance_after_transaction"),
    to_json(col("transaction_items")).alias("transaction_items_json"),
    to_json(col("shipping_info")).alias("shipping_info_json"),
    col("customer_feedback"),
    col("discount_applied")
)

# Write to CSV
df_json.coalesce(1).write.csv(
    path="D:\SparkProjects\TestData\data-raw",
    mode="overwrite",
    header=True
)

df_with_total_price = df_flat.withColumn(
    "total_item_price", 
    F.col("unit_price") * F.col("quantity")
)
df_account_summary = df_with_total_price.groupBy("transaction_id",
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
        "shipping_address",
        "shipping_method",
        "shipping_cost",
        "estimated_delivery",
        "tracking_number",
        "customer_feedback",
        "discount_applied").agg(
    F.concat_ws(",", F.collect_list("product_name")).alias("products_list"),
    F.sum("total_item_price").alias("total_price_spent"),
    F.sum("quantity").alias("total_quantity_bought")
)
df_account_summary.coalesce(1).write.csv(
    path="D:\SparkProjects\TestData\data-processed",
    mode="overwrite",
    header=True
)