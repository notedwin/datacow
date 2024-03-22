import csv
import os

import googlemaps
from dotenv import find_dotenv, load_dotenv

"""
Using a place_id (329xx8a) get additional information about a place and save the data to a CSV file.
Once the script is run once, the output csv is read and used as a cache to avoid making duplicate API calls.
"""


load_dotenv(find_dotenv())
key = os.getenv(key="GOOGLE_API")

gmaps = googlemaps.Client(key)
places_file = "places_id.csv"
info_file = "info.csv"
places_to_id = {}
processed = []
new_id = False
cols = []

fields = [
    "formatted_address",
    "geometry/location/lng",
    "geometry/location/lat",
    "icon",
    "name",
    "place_id",
    "rating",
    "user_ratings_total",
    "type",
    "url",
    "rating",
    "website",
    "price_level",
]


with open(places_file, mode="r") as f:
    reader = csv.reader(f)
    places_to_id = dict(reader)

with open(info_file, mode="r") as f:
    reader = csv.reader(f)
    cols = next(reader)
    for row in reader:
        processed.append(row[5])

print(processed)
# res = gmaps.place("ChIJVVVQesrSD4gRqhYvIJm_YJM", fields=fields)
# row = {}

with open(info_file, "a") as f:
    w = csv.DictWriter(f, cols)
    for key, value in places_to_id.items():
        if value not in processed:
            res = gmaps.place(value, fields=fields)
            row = {}
            if res.get("status") == "OK":
                for key, value in res["result"].items():
                    if key == "geometry":
                        row["lat"] = value["location"]["lat"]
                        row["lon"] = value["location"]["lng"]
                    else:
                        row[key] = value
            w.writerow(row)
            processed.append(key)
        else:
            print("no need")
