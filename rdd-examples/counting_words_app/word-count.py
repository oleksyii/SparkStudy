import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///C:/SparkCourse/apps/counting_words_app/Book.txt")
words = input.flatMap(
    lambda x: re.compile(r"\b\W+(?:'\W+)?\b", re.UNICODE).split(x.lower())
)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountSorted.collect()

for result in results:
    print(f"{str(result[0])} {result[1].encode('ascii', 'ignore').decode()}")
