"""
Spark program for streaming and processing tweets.
"""
from requests import post
from pyspark import SparkContext, StorageLevel
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from config import CHECKPOINT, ADDRESS, PORT

# url used for dashboard visualization
dashboard_url = 'http://localhost:5001/updateData'


def aggregate_tags_count(new_values, total_sum):
    """
    Function to aggregate hashtag counts.
    :param new_values: The new values to be added to the running total.
    :param total_sum: A running total of values.
    :return: The sum of the new values and previous values.
    """
    if total_sum is None:
        total_sum = 0
    return sum(new_values) + total_sum


def get_sql_context_instance(spark_configuration):
    """
    Lazily instantiated global instance of SparkSession.
    :param spark_configuration: Configuration of a SparkSession.
    :return: Instantiated instance of SparkSession.
    """
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=spark_configuration).getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def create_context(host, port):
    """
    Function to create and set up a new streaming context.
    :param host: A string describing the IP to use for HTTP Client. Example: 'localhost'
    :param port: An integer value that corresponds to the port number used to read data from.
    :return: The created and configured streaming context.
    """
    spark_context = SparkContext(master="local[2]", appName="TwitterStreamApp")
    spark_context.setLogLevel("ERROR")
    # create the Streaming Context from the above spark context with interval size 2 seconds
    streaming_context = StreamingContext(spark_context, 2)
    # setting a checkpoint to allow RDD recovery
    streaming_context.checkpoint(CHECKPOINT)
    # read data from port 9009
    data_stream = streaming_context.socketTextStream(ADDRESS, PORT,
                                                     storageLevel=StorageLevel(True, True, False, False, 2))
    # split each tweet into words
    words = data_stream.flatMap(lambda line: line.split(" "))
    # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
    hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
    # adding the count of each hashtag to its last count
    tags_totals = hashtags.updateStateByKey(aggregate_tags_count)

    def process_rdd(time, rdd):
        """
        Function that processes an RDD.
        :param time: Time stamp of the process.
        :param rdd: The RDD to be processed.
        """
        print("----------- %s -----------" % str(time))
        try:
            if rdd:
                # Get spark sql singleton context from the current context
                sql_context = get_sql_context_instance(rdd.context.getConf())
                # convert the RDD to Row RDD
                row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
                # create a DF from the Row RDD
                hashtags_dataframe = sql_context.createDataFrame(row_rdd)
                # Register the dataframe as table
                hashtags_dataframe.createOrReplaceTempView("hashtags")
                # get the top 10 hashtags from the table using SQL and print them
                hashtag_counts_dataframe = sql_context.sql(
                    "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
                hashtag_counts_dataframe.show()

                # call this method to prepare top 10 hashtags DF and send them

                def send_dataframe_to_dashboard(dataframe):
                    """
                    Function to send DataFrame to the dashboard for visualization.
                    :param dataframe: Spark DataFrame created by process_rdd().
                    """
                    # extract the hashtags from dataframe and convert them into array
                    top_tags = [str(t.hashtag) for t in dataframe.select("hashtag").collect()]
                    # extract the counts from dataframe and convert them into array
                    tags_count = [p.hashtag_count for p in dataframe.select("hashtag_count").collect()]
                    # initialize and send the data through REST API
                    request_data = {'label': str(top_tags), 'data': str(tags_count)}
                    response = post(dashboard_url, data=request_data)

                send_dataframe_to_dashboard(hashtag_counts_dataframe)
        except:
            pass

    # do processing for each RDD generated in each interval
    tags_totals.foreachRDD(process_rdd)
    return streaming_context


if __name__ == "__main__":
    # create streaming context
    streaming_context = StreamingContext.getOrCreate(CHECKPOINT, lambda: create_context(ADDRESS, PORT))
    # start the streaming computation
    streaming_context.start()
    # wait for the streaming to finish
    streaming_context.awaitTermination()
