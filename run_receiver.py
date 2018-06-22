"""
RUN FILE FOR STREAMING
code adapted from tutorial by Hanee Medhat
https://www.toptal.com/apache/apache-spark-streaming-twitter
"""
from listener import Listener
from config import ADDRESS, PORT
from twitter_stream import TwitterStream


if __name__ == "__main__":
    # make a Listener
    listener = Listener(ADDRESS, PORT)
    # connect to Twitter API and Spark
    listener.echo_server()




