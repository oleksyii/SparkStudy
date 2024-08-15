from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    id = fields[0]
    # item_id = fields[1]
    spent = float(fields[2])
    return (id, spent)


lines = sc.textFile(
    "file:///C:/SparkCourse/apps/total_amount_spent_app/customer-orders.csv"
)
rdd = lines.map(parseLine)
totalsByAge = rdd.reduceByKey(lambda x, y: x + y)
result = totalsByAge.sortBy(lambda x: x[1]).collect()
for r in result:
    print(r)


# OR


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import sum

# # Initialize a Spark session
# spark = SparkSession.builder.master("local").appName("FriendsByAge").getOrCreate()

# # Read the CSV file into a DataFrame
# df = spark.read.csv(
#     "file:///C:/SparkCourse/apps/total_amount_spent_app/customer-orders.csv",
#     header=True,
#     inferSchema=True,
# )

# # Group by age and calculate the average number of friends
# total_spent_by_customer = df.groupBy("cust_id").agg(sum("amount").alias("TotalSpent"))

# # Collect the results and print
# total_spent_by_customer.sort("cust_id").show(
#     total_spent_by_customer.count(), truncate=False
# )

# # Stop the Spark session
# spark.stop()
