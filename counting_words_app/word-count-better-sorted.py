import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///C:/SparkCourse/apps/counting_words_app/Book.txt")
pattern = re.compile(r"\w+(?:'\w+)?", re.UNICODE)


def split_text(text):
    matches = pattern.findall(text.lower())
    # Filter out None values and keep only non-word characters
    non_word_matches = [match for match in matches if match]
    return non_word_matches


words = input.flatMap(split_text)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountSorted.collect()

for result in results:
    print(f"{str(result[0])} {result[1].encode('ascii', 'ignore').decode()}")
