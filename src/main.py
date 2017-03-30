from datetime import datetime
from decimal import Decimal

import time

import requests
import boto3

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
    else:
        return r.json()


def store(table, data):
    """Saves data to dynamoDB
    params  : -
    returns : -
    """
    ddb = boto3.resource("dynamodb")
    t = ddb.Table(table)
    with t.batch_writer() as batch:
        for d in data:
            del d["contract_name"]
            del d["status"]
            del d["bonus"]

            d["position"]["lng"] = Decimal(str(d["position"]["lng"]))
            d["position"]["lat"] = Decimal(str(d["position"]["lat"]))

            batch.put_item(Item=d)


def main():
    while True:
        data = fetch_data(conf.URL + conf.API_KEY)
        if not data:
            logger.log(datetime.now(), "GET ERROR")
        else:
            store(conf.TABLE_NAME, data)
        time.sleep(60 * 5)


if __name__ == "__main__":
    for c in [conf.API_KEY, conf.URL, conf.TABLE_NAME, conf.LOG_FILE]:
        if not c:
            raise SystemExit("Invalid Config")
    logger = Logger(conf.LOG_FILE)
    main()
