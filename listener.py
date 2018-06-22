"""
Listener class
"""
from socket import socket, AF_INET, SOCK_STREAM, gaierror, error
from sys import exc_info
from config import PARAMS, URL
from twitter_stream import TwitterStream


class Listener:
    def __init__(self, address, port):
        """
        Initializes a Listener with specified IP and port number for HTTP Client; creates socket.
        :param address: A string describing the IP to use for HTTP Client. Example: 'localhost'
        :param port: An integer value that corresponds to the port number used for TCP connection.
        """
        self.address = address
        self.port = port
        self.client_sock = None
        self.sock = None
                
    def echo_handler(self):
        """
        Function to handle connection between Twitter API and Spark.
        """
        try:
            # make TwitterStream
            twitter_stream = TwitterStream()
            # construct query_url
            twitter_stream.construct_query_url(URL, PARAMS)
            # get tweets
            twitter_response = twitter_stream.get_tweets()
            # send tweets to spark (uses the tcp connection from listener)
            twitter_stream.send_tweets_to_spark(self.client_sock)
        except error:
            e = exc_info()
            print("Handler Error:", e)

    def echo_server(self, backlog=1):
        """
        Function to create socket; calls handler function to make the connection.
        :param backlog: Integer value representing the specified number of unaccepted connections
                        allowed before refusing new connections.
        """
        try:
            self.sock = socket(AF_INET, SOCK_STREAM)
        except error:
            e = exc_info()
            print("Error creating the socket:", e)
            exit(1)
        try:
            self.sock.bind((self.address, self.port))
            self.sock.listen(backlog)
            print("Waiting for TCP connection...")
            while True:
                self.client_sock, client_addr = self.sock.accept()
                self.echo_handler()
        except gaierror:
            e = exc_info()
            print("Address-related error connecting to server: ", e)
            exit(1)
        except OSError:
            e = exc_info()
            print("Address in use:", e)
            exit(1)
        except error:
            e = exc_info()
            print("Connection error: ", e)
            exit(1)
