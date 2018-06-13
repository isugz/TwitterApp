import json
from _socket import error
from sys import exc_info
from requests import get, exceptions
from listener import AUTH
from socket import socket

"""
Twitter Stream Class
"""


class TwitterStream:
    def __init__(self):
        """
        Initializes a TwitterStream with an empty response and query_url
        """
        self.query_url = ''
        self.response = None

    def construct_query_url(self, url, query_data):
        """
        Function to construct the query_url for the TwitterStream.
        :param url: A string representing the correct Twitter API url.
        :param query_data: A list of tuples to filter tweets by.
                            Example: query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
        """
        self.query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])

    def get_tweets(self):
        """
        A function to get the response from the Twitter API.
        :return: response containing tweets from Twitter API.
        """
        try:
            self.response = get(self.query_url, auth=AUTH, stream=True)
            print(self.query_url, self.response)
            return self.response
        except exceptions.HTTPError as e:
            print(e)
            exit(1)

    def send_tweets_to_spark(self, tcp_connection):
        """
        A function to send tweets to Spark for processing.
        :param tcp_connection: A socket connection for the Twitter API.
        """
        num_tweets = 0
        for line in self.response.iter_lines():
            if not line.decode('utf-8'):
                continue
            try:
                full_tweet = json.loads(line.decode('utf-8'))
                tweet_text = full_tweet['text']
                print("Tweet Text: " + tweet_text, '\n', '-' * 20)
                num_tweets += 1
                print("successful tweets:", num_tweets)
                tcp_connection[0].send(tweet_text.encode('utf-8'))
            except error:
                e = exc_info()
                print("Error sending:", e)
                exit(1)
            except ConnectionError:
                e = exc_info()
                print("Connection error:", e)
                exit(1)

