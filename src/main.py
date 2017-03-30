from datetime import datetime
import json

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


def save(data):
    """Saves data to dynamoDB"""
    # initiate AWS service
    dynamodb = boto3.resource('dynamodb')
    # set the table for storing data
    table = dynamodb.Table('DublinBikes')
    # open json file
    with open(data) as json_file:
        bikes = json.load(json_file, parse_float=decimal.Decimal)
        for obj in bikes:
            name = obj["name"]
            address = obj["address"]
            lat = float(obj["position"]["lat"])
            time_stamp = obj['last_update']
            lna = float(obj["position"]["lng"])
            free = int(obj['available_bikes'])
            number = int(obj["number"])
            bike_stands = int(obj["bike_stands"])
            available_bike_stands = int(obj['available_bike_stands'])
            print("Adding bike occupancy data:", name,
                  "free bikes: ", free, "from", number)

            table.put_item(
                Item={
                    'name': name,
                    'id': id,
                    'lat': lat,
                    'timestamp': time_stamp,
                    'lna': lna,
                    'free': free,
                    'number': number,
                    "bike_stands": bike_stands,
                    "available_bike_stands": available_bike_stands,
                    "address": address
                }
            )
    print("PutItem succeeded:")


def main():
    while True:
        data = fetch_data(conf.URL + conf.API_KEY)
        print(data)
        if not data:
            logger.log(datetime.now(), "GET ERROR")
        else:
            save(data, conf.DATA_FILE)
        time.sleep(60 * 5)

if __name__ == "__main__":
    for c in [conf.API_KEY, conf.URL, conf.DATABASE_NAME,
              conf.COLLECTION_NAME, conf.LOG_FILE]:
        if not c:
            raise SystemExit("Invalid Config")
    logger = Logger(conf.LOG_FILE)
    main()
