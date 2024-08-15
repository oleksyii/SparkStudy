from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import ArrayType, StringType
import re

spark = SparkSession.builder.appName("WordCount").getOrCreate()

pattern = re.compile(r"\w+(?:'\w+)?", re.UNICODE)


def split_text(text):
    matches = pattern.findall(text.lower())
    # Filter out None values and keep only words
    word_matches = [match for match in matches if match]
    return word_matches


# Register the function as a UDF
split_text_udf = func.udf(split_text, ArrayType(StringType()))

# Read each line of my book into a dataframe
inputDF = spark.read.text("file:///C:/SparkCourse/apps/SparkSQL/task4/book.txt")

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(split_text_udf(inputDF.value)).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(
    func.lower(wordsWithoutEmptyString.word).alias("word")
)

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())
