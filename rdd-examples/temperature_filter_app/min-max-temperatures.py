from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    # Farenheit formula
    # temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    temperature = float(fields[3]) * 0.1
    return (stationID, entryType, temperature)


lines = sc.textFile("file:///C:/SparkCourse/apps/temperature_filter_app/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()

print(f"{'MIN TEMPERATURES':-^50}")
for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))


results = (
    parsedLines.filter(lambda x: "TMAX" in x[1])
    .map(lambda x: (x[0], x[2]))
    .reduceByKey(lambda x, y: max(x, y))
    .collect()
)
print(f"{'MAX TEMPERATURES':-^50}")
for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))
