from pyspark.sql import SparkSession
import collections
import asyncio
from time import time, perf_counter
from pathlib import Path
import concurrent.futures


def blocks(n):
    # Initialize a Spark session
    spark = (
        SparkSession.builder.master("local").appName("RatingsHistogram").getOrCreate()
    )

    # Read the CSV file into a DataFrame
    df = spark.read.csv(
        "file:///SparkCourse/ml-100k-new/ratings.csv", header=True, inferSchema=True
    )
    print("read the data from a file")

    # Select the 'rating' column
    ratings = df.select("rating")
    print("selected column of interest")

    # Count the number of occurrences of each rating
    result = ratings.groupBy("rating").count().collect()
    print("done counting the entrance of each unique score in data")

    # Sort the results by rating value
    sortedResults = collections.OrderedDict(sorted(result, key=lambda x: x["rating"]))
    print("sorted the results for the output")

    # Stop the Spark session
    spark.stop()
    return sortedResults


async def monitoring():
    start = perf_counter()
    while True:
        await asyncio.sleep(1)
        print(f"Monitoring time passed: {perf_counter()-start:.4f}")


async def run_blocking_calculations(executor, file: Path):
    loop = asyncio.get_event_loop()
    print("waiting for executor tasks")
    result = await loop.run_in_executor(executor, blocks, file)

    return result


async def main(file: Path):
    asyncio.create_task(monitoring())
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = run_blocking_calculations(executor, file)
        results = await future
        return results


if __name__ == "__main__":
    p = Path("file:///SparkCourse/ml-100k-new/ratings.csv")
    result = asyncio.run(main(p))
    # Print the results
    for row in result:
        print("%s %s" % (row, result[row]))

    # # Plot the data
    # plt.figure(figsize=(8, 6))
    # # For example, a bar plot
    # plt.bar([row for row in result], [result[row] for row in result])
    # # Add titles and labels
    # plt.xlabel("Score")
    # plt.ylabel("Num of ratings")
    # plt.title("Num of scores agains ratings")
    # # Show the plot
    # plt.show()
