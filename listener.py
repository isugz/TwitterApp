from socket import socket, AF_INET, SOCK_STREAM, gaierror, error
from sys import exc_info
from requests_oauthlib import OAuth1
from http_config import *

# Twitter API configurations
ACCESS_TOKEN = twitter_access_token
ACCESS_SECRET = twitter_access_secret
CONSUMER_KEY = twitter_consumer_key
CONSUMER_SECRET = twitter_consumer_secret

# Twitter API Authorization
AUTH = OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

"""
Twitter Stream Listener class
"""


class Listener:
    def __init__(self, tcp_ip, tcp_port):
        """
        Initializes a Listener with specified IP and port number for HTTP Client; creates socket.
        :param tcp_ip: A string describing the IP to use for HTTP Client. Example: 'localhost'
        :param tcp_port: An integer value that corresponds to the port number used for TCP connection.
        """
        self.tcp_ip = tcp_ip
        self.tcp_port = tcp_port
        self.connection = None
        try:
            self.sock = socket(AF_INET, SOCK_STREAM)
        except error:
            e = exc_info()
            print("Error creating the socket:", e)
            exit(1)

    def start_tcp_connection(self):
        """
        This function establishes the TCP connection after the Listener has been initialized.
        """
        try:
            self.sock.bind((self.tcp_ip, self.tcp_port))  # binding to (host, port)
            self.sock.listen(1)
            print("Waiting for TCP connection...")
            self.connection = self.sock.accept()
            print("Connected... Starting getting tweets.")
        except gaierror:
            e = exc_info()
            print("Address-related error connecting to server: ", e)
            exit(1)
        except error:
            e = exc_info()
            print("Connection error: ", e)
            exit(1)
