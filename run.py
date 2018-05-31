# Quickstart tutorial to review using Datasets instead of RDD's

from os import environ
from pkg_resources._vendor.pyparsing import col
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# this uses RDD; should use Dataset
# logfile = environ['SPARK_HOME'] + "/README.md"
# sc = SparkContext("local", "TwitterAppBackend")
# logData = sc.textFile(logfile).cache()
#
# number_of_As = logData.filter(lambda s: 'a' in s).count()
# number_of_Bs = logData.filter(lambda s: 'b' in s).count()
#
# print("Lines with a: %i, lines with b: %i" % (number_of_As, number_of_Bs))
#
# Using Dataset instead of RDD
spark = SparkSession.builder.appName("TwitterStreamingApp").config("spark.some.config.option", "some-value").getOrCreate()
logfile = environ['SPARK_HOME'] + '/README.md'
logData = spark.read.text(logfile).cache()
print(logData.count(), logData.first())

# textFile.filter(textFile.value.contains("Spark")).count()
# note: the column access must be done in a sql fashion before you can access its functions
print(logData.filter(logData["value"].contains("Spark")).count())

# textFile.select(size(split(textFile.value, "\s+")).name("numWords")).agg(max(col("numWords"))).collect()
print(logData.select(size(split(logData["value"], "\s+")).name("numWords")).agg(max(col("numWords"))).collect())

# One common data flow pattern is MapReduce, as popularized by Hadoop. Spark can implement MapReduce flows easily:
# textFile.select(explode(split(textFile.value, "\s+")).alias("word")).groupBy("word").count()
word_count = logData.select(explode(split(logData["value"], "\s+")).alias("word")).groupBy("word").count()
print(word_count.collect())

# now back to counting a's and b's
number_of_As = logData.filter(logData["value"].contains('a')).count()
number_of_Bs = logData.filter(logData["value"].contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (number_of_As, number_of_Bs))

# always stop the program
spark.stop()
