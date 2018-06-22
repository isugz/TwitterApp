"""
Configuration file handling OAuth, host, port, url, and checkpoint settings.
"""
from http_config import *
from requests_oauthlib import OAuth1

# Twitter API configurations
ACCESS_TOKEN = twitter_access_token
ACCESS_SECRET = twitter_access_secret
CONSUMER_KEY = twitter_consumer_key
CONSUMER_SECRET = twitter_consumer_secret

# Twitter API Authorization
AUTH = OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
# variables: tcp_ip, tcp_port, checkpoint, url, query_data
ADDRESS, PORT, CHECKPOINT = "localhost", 9009, "checkpoint_TwitterApp"
URL, PARAMS = 'https://stream.twitter.com/1.1/statuses/filter.json', \
                  [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
              # {
              #       'language': 'en',
              #       'locations': '-130, -20, 100, 50',
              #       'track': '#'
              #     }
