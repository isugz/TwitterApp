from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext, SparkSession
import sys
import requests



def aggregate_tags_count(new_values, total_sum):
    if total_sum is None:
        total_sum = 0
    return sum(new_values) + total_sum


def get_sql_context_instance(spark_configuration):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=spark_configuration).getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        if rdd:
            # Get spark sql singleton context from the current context
            sql_context = get_sql_context_instance(rdd.context.getConf())
            # convert the RDD to Row RDD
            row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
            # create a DF from the Row RDD
            hashtags_df = sql_context.createDataFrame(row_rdd)
            # Register the dataframe as table
            # hashtags_df.registerTempTable("hashtags")
            hashtags_df.createOrReplaceTempView("hashtags")
            # get the top 10 hashtags from the table using SQL and print them
            hashtag_counts_df = sql_context.sql(
                "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
            hashtag_counts_df.show()
            # call this method to prepare top 10 hashtags DF and send them
            send_df_to_dashboard(hashtag_counts_df)
    except:
        pass


def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)

# create spark configuration
if __name__ == "__main__":
    spark_context = SparkContext(master="local[2]", appName="TwitterStreamApp")
    spark_context.setLogLevel("ERROR")
    # create the Streaming Context from the above spark context with interval size 2 seconds
    streaming_context = StreamingContext(spark_context, 2)
    # setting a checkpoint to allow RDD recovery
    streaming_context.checkpoint("checkpoint_TwitterApp")
    # read data from port 9009
    dataStream = streaming_context.socketTextStream("localhost", 9009)
    # split each tweet into words
    words = dataStream.flatMap(lambda line: line.split(" "))
    # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
    hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
    # adding the count of each hashtag to its last count
    tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
    # do processing for each RDD generated in each interval
    tags_totals.foreachRDD(process_rdd)
    # start the streaming computation
    streaming_context.start()
    # wait for the streaming to finish
    streaming_context.awaitTermination()


















































