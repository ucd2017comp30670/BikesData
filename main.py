from datetime import datetime
import time

# from bson import BSON
# from pymongo import MongoClient
import requests

import config as conf


class Logger:
    """"""

    def __init__(self, log_file):
        self.log_file = log_file

    def log(self, timestamp, message):
        with open(self.log_file, "at") as f:
            f.write("{} : {}\n".format(timestamp, message))


def fetch(url):
    """"""
    try:
        r = requests.get(url)
    except requests.ConnectionError:
        logger.log(datetime.now, "Connection Error")
    if r.json():
        return dict(r.json())


def store(dictionary):
    """"""
    print(dictionary)


def main():
    data = fetch(conf.URL + conf.API_KEY,
                 conf.DATABASE_NAME,
                 conf.COLLECTION_NAME)


if __name__ == "__main__":
    logger = Logger(conf.LOG_FILE)
    main()
