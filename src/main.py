from datetime import datetime
import time
import requests
import boto.dynamodb
import config as conf
import decimal


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


def createTable(db):
    "Function to create new table in dynamoDB"
    # set the schema for table
    DublinBikes_table_schema = db.create_schema(
        hash_key_name='name',
        hash_key_proto_value=str,
        range_key_name='time_stamp',
        range_key_proto_value=int
    )
    # create table
    db.create_table(
        name='DublinBikes',
        schema=DublinBikes_table_schema,
        read_units=25,
        write_units=25
    )


def save(data):
    """Saves data to dynamoDB, each location's attributes in specific time is saved as a new item in the table"""

    # connect to  AWS DB service
    db = boto.dynamodb.connect_to_region("eu-west-1",
                                         aws_access_key_id=conf.ACESS_KEY,
                                         aws_secret_access_key=conf.SECRET_KEY
                                         )
    db.use_decimals()
    count = 0

    for obj in data:
        name = obj["name"]
        address = obj["address"]
        lat = decimal.Decimal(str(obj["position"]["lat"]))
        time_stamp = obj['last_update']
        lna = decimal.Decimal(str(obj["position"]["lng"]))
        free = obj['available_bikes']
        number = obj["number"]
        bike_stands = obj["bike_stands"]
        available_bike_stands = obj['available_bike_stands']
        count += 1
        item_data = {
            "name": name,
            "address": address,
            "lat": lat,
            "lna": lna,
            "time_stamp": time_stamp,
            "free": free,
            "number": number,
            "bike_stands": bike_stands,
            "available_bike_stands": available_bike_stands,
            "count": count
        }
        table = db.get_table('DublinBikes')
        item = table.new_item(
            # primary key
            hash_key=name,
            # range key
            range_key=time_stamp,
            # This has the
            attrs=item_data
        )
        item.put()
        print("Adding bike occupancy data from:", name,
              "free bikes at the moment: ", free, "from", number)

    print("Put Items succeeded:")


def main():
    while True:
        data = fetch_data(conf.URL + conf.API_KEY)
        print(data)
        if not data:
            logger.log(datetime.now(), "GET ERROR")
        else:
            save(data)
        time.sleep(60 * 5)

if __name__ == "__main__":
    for c in [conf.API_KEY, conf.URL, conf.DATABASE_NAME,
              conf.COLLECTION_NAME, conf.LOG_FILE]:
        if not c:
            raise SystemExit("Invalid Config")
    logger = Logger(conf.LOG_FILE)
    main()
