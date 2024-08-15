from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import col, avg

# Initialize a Spark session
spark = SparkSession.builder.master("local").appName("FriendsByAge").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv(
    "file:///C:/SparkCourse/apps/SparkSQL/task3/fakefriends-header.csv",
    header=True,
    inferSchema=True,
)

# Group by age and calculate the average number of friends
averagesByAge = (
    df.select("age", "friends")
    .groupBy("age")
    .agg(func.round(avg("friends"), 2).alias("average_friends"))
    .sort("age")
)

# Collect the results and print
averagesByAge.show(averagesByAge.count(), truncate=False)

# Stop the Spark session
spark.stop()
