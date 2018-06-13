# RUN FILE FOR STREAMING
# code adapted from tutorial by Hanee Medhat
# https://www.toptal.com/apache/apache-spark-streaming-twitter
from listener import Listener
from twitter_stream import TwitterStream

# variables: tcp_ip, tcp_port, url, query_data
TCP_IP, TCP_PORT = "localhost", 9009
URL, QUERY_DATA = 'https://stream.twitter.com/1.1/statuses/filter.json', \
                  [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]

if __name__ == "__main__":
    # make a Listener
    listener = Listener(TCP_IP, TCP_PORT)
    # start tcp connection
    listener.start_tcp_connection()
    # make TwitterStream
    # tcp_connection = listener.connection
    twitter_stream = TwitterStream()
    # construct query_url
    twitter_stream.construct_query_url(URL, QUERY_DATA)
    # get tweets
    # TODO: handle if response is not 200 in the TwitterStream class
    twitter_response = twitter_stream.get_tweets()
    # send tweets to spark (uses the tcp connection from listener)
    twitter_stream.send_tweets_to_spark(listener.connection)



