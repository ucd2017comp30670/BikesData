from datetime import datetime
import time

from pymongo import MongoClient
import requests

import config as conf


class Logger:
    """A simple logger"""

    def __init__(self, log_file):
        self.log_file = log_file

    def log(self, timestamp, message):
        """writes timestamp and message to file
        params  : timestamp (), message (str)
        returns : -
        """
        with open(self.log_file, "at") as f:
            f.write("{} : {}\n".format(timestamp, message))


def fetch_data(url):
    """Returns a list of dicts returned by a get request to a url that returns JSON
    params  : url (str)
    returns : [dict]
    """
    try:
        r = requests.get(url)
    except requests.ConnectionError:
        logger.log(datetime.now, "Connection Error")
    return r.json()


def store(data, database, collection):
    """Saves a list of dictionary objects in a mongoDB collection
    params  : data [dict], database (str), collection (str)
    returns : pymongo.results.Insert_one_result
    """
    mc = MongoClient()[database][collection]
    r = mc.insert_many(data)
    return r


def main():
    while True:
        data = fetch_data(conf.URL + conf.API_KEY)
        if not data:
            logger.log(datetime.now(), "GET ERROR")
        else:
            id_ = store(data, conf.DATABASE_NAME, conf.COLLECTION_NAME)
            if not id_:
                logger.log(datetime.now(), "Error Writing {}".format(data))
        time.sleep(60 * 5)

if __name__ == "__main__":
    for c in [conf.API_KEY, conf.URL, conf.DATABASE_NAME,
              conf.COLLECTION_NAME, conf.LOG_FILE]:
        if not c:
            raise SystemExit("Invalid Config")
    logger = Logger(conf.LOG_FILE)
    main()
