'''
Created on 30 Mar 2017

@author: liga
'''
from main import save
from nose.tools import *


def test_db(data):
    for obj in data:
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
