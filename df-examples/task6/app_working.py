from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)
from pyspark.sql.functions import sum

# Initialize a Spark session
spark = SparkSession.builder.master("local").appName("FriendsByAge").getOrCreate()
schema = StructType(
    [
        StructField("cust_id", StringType(), True),
        StructField("item_id", StringType(), True),
        StructField("amount", FloatType(), True),
    ]
)
# Read the CSV file into a DataFrame
df = spark.read.schema(schema).csv(
    "file:///C:/SparkCourse/apps/SparkSQL/task6/customer-orders.csv"
)

# Group by age and calculate the average number of friends
total_spent_by_customer = (
    df.select("cust_id", "amount")
    .groupBy("cust_id")
    .agg(func.round(sum("amount"), 2).alias("TotalSpent"))
    .sort("TotalSpent")
)

# Collect the results and print
total_spent_by_customer.show(total_spent_by_customer.count(), truncate=False)

# Stop the Spark session
spark.stop()
