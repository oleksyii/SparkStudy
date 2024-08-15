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

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("item_id", StringType(), True),
        StructField("amount_spent", FloatType(), True),
    ]
)

# // Read the file as dataframe
df = spark.read.schema(schema).csv(
    "file:///C:/SparkCourse/apps/SparkSQL/task6/customer-orders.csv"
)
df.printSchema()
result = (
    df.select("customer_id", "amount_spent")
    .groupBy("customer_id")
    .agg(func.round(sum("amount_spent"), 2).alias("total_spending"))
    .sort("total_spending")
)

result.show(result.count())

spark.stop()
