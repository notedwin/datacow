import csv

import requests
from bs4 import BeautifulSoup

"""
Script to scrape labels from chicago hoodmaps website and save the data to a CSV file.
"""


url = "https://hoodmaps.com/chicago-neighborhood-map"
filename = "data/hoodmaps.html"
csv_file = "data/hoods.csv"

try:
    with open(filename, "r", encoding="utf-8") as file:
        html = file.read()
except FileNotFoundError:
    response = requests.get(url)
    if response.status_code == 200:
        html = response.text
        with open(filename, "w", encoding="utf-8") as file:
            file.write(html)
    else:
        print(f"Failed to fetch the URL. Status code: {response.status_code}")
        exit()

soup = BeautifulSoup(html, "html.parser")

body_content = soup.find("body")

div_meta = body_content.find("div", {"class": "meta"})
list_ = body_content.find("ul")
with open(csv_file, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["Name", "Latitude", "Longitude", "Votes"]
    csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    csv_writer.writeheader()
    for place in list_.find_all("li"):
        latitude = place.find("meta", {"itemprop": "latitude"})["content"]
        longitude = place.find("meta", {"itemprop": "longitude"})["content"]
        name = place.find("span", {"itemprop": "name"}).text.strip()
        votes = int(place.text.split("(")[1].split()[0])
        if votes > 15:
            csv_writer.writerow(
                {
                    "Name": name,
                    "Latitude": latitude,
                    "Longitude": longitude,
                    "Votes": votes,
                }
            )
