# RUN FILE FOR STREAMING
# code adapted from tutorial by Hanee Medhat
# https://www.toptal.com/apache/apache-spark-streaming-twitter
from listener import Listener, TCP_IP, TCP_PORT, URL, QUERY_DATA
from twitter_stream import TwitterStream


if __name__ == "__main__":
    # make a Listener
    listener = Listener(TCP_IP, TCP_PORT)
    # start tcp connection
    listener.start_tcp_connection()
    # make TwitterStream
    twitter_stream = TwitterStream()
    # construct query_url
    twitter_stream.construct_query_url(URL, QUERY_DATA)
    # get tweets
    twitter_response = twitter_stream.get_tweets()
    # send tweets to spark (uses the tcp connection from listener)
    twitter_stream.send_tweets_to_spark(listener.connection)



