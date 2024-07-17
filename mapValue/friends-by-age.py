from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1])
)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)


# OR


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg

# # Initialize a Spark session
# spark = SparkSession.builder.master("local").appName("FriendsByAge").getOrCreate()

# # Read the CSV file into a DataFrame
# df = spark.read.csv(
#     "file:///SparkCourse/fakefriends.csv", header=True, inferSchema=True
# )

# # Group by age and calculate the average number of friends
# averagesByAge = df.groupBy("Age").agg(avg("Friends").alias("AverageFriends"))

# # Collect the results and print
# averagesByAge.show()
# results = averagesByAge.collect()
# for result in results:
#     print(result)

# # Stop the Spark session
# spark.stop()
