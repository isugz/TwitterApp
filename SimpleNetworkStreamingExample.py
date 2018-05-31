from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# create local streaming context with two working thread and a batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# create a DStream that will connect to hostname:port
lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# print the first ten elements of each RDD generated in this DStream
wordCounts.pprint()

# start the computation
ssc.start()

# wait for the computation to terminate
ssc.awaitTermination()

# open terminal and run nc -lk 9999
# start typing!


