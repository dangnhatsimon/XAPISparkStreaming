import tweepy
from tweepy import OAuthHandler
from tweepy.streaming import StreamingClient, StreamResponse
import socket
import json
import logging

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

with open("D:/Spark/XAPISparkStreaming/app.json", "+rb") as settings:
    x_settings = json.load(settings)
    consumer_key = x_settings.get("client_id")
    consumer_secret = x_settings.get("client_secret")
    access_token = x_settings.get("access_token")
    access_token_secret = x_settings.get("access_token_secret")


class TweetListener(StreamResponse):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            logging.info(msg["text"].encode("utf-8"))
            self.client_socket.send(msg["text"].encode("utf-8"))
            return True
        except BaseException as e:
            logging.error(f"Error: {e}")
        return True

    def on_error(self, status):
        logging.info(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = StreamingClient(auth, TweetListener(c_socket))
    twitter_stream.filter(track=["guitar"])


if __name__ == "__main__":
    host = "127.0.0.1"
    port = 9999
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(5)
    logging.info(f"Listened for connection on {host}:{port}.")
    conn, addr = s.accept()

    sendData(conn)
