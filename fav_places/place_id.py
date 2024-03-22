import csv
import os

import googlemaps
from dotenv import find_dotenv, load_dotenv

"""
Script to get place_id (329xx8a) from place name (Chicago Field Museum) and save the data to a CSV file.
Once the script is run once, the csv is read and used as a cache to avoid making duplicate API calls.
"""


load_dotenv(find_dotenv())
gsecret = os.getenv("API_KEY")

gmaps = googlemaps.Client(gsecret)

places_file = "places_id.csv"
gt_file = "saved_places_gt.csv"
places_to_id = {}
read_in_places = []
new_address = False

with open(places_file, mode="r") as f:
    reader = csv.reader(f)
    places_to_id = dict(reader)

with open(gt_file, mode="r") as f:
    reader = csv.reader(f)
    next(reader)  # ignore header
    for row in reader:
        read_in_places.append(row[0])

print(places_to_id, read_in_places)

for place in read_in_places:
    if place not in places_to_id:
        new_address = True
        geocode_result = gmaps.find_place(
            place, "textquery", location_bias="circle:50@41.872144, -87.653159"
        )
        place_id = geocode_result["candidates"][0]["place_id"]
        places_to_id[place] = place_id
    else:
        print("no api call")

if new_address:
    print(new_address)
    with open("places_id.csv", "w") as f:
        [f.write("{0},{1}\n".format(key, value)) for key, value in places_to_id.items()]
