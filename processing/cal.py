
# cache this and run daily

import graphene
from datetime import datetime
import json
import requests
import os
from sqlalchemy import create_engine
from sqlalchemy import inspect
import logging
import dotenv

from data_utilities import dump_api_json

dotenv.load_dotenv()

# query leetcode api to get questions answered today by username notedwin
# the benefit of running this in a pipeline as oposed to directly queriying and manipulating the data 
# in js, is that the data is relatively static for the day
# deciding what format to store the data is a bit of a problem

# I am trying to decide what format to store API json reposnes in my SQL database. i will be using the data in the database to create a dashboard


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("etl")
engine = create_engine(os.getenv(key="DATABASE_URL"))
inspector = inspect(engine)
gh_token = os.getenv(key="GH_TOKEN")
lc_query = """query 
{ 
    matchedUser(username: "notedwin") 
    {
        submissionCalendar
    }
}"""

gh_query = """query {
    viewer {
        login
        contributionsCollection {
            contributionCalendar {
                totalContributions
                weeks {
                    contributionDays {
                        contributionCount
                        date
                    }
                }
            }
        }
    }
}"""


# get data and normalize it from leetcode
result = requests.post("https://leetcode.com/graphql", json={"query": lc_query}).json()[
    "data"
]["matchedUser"]["submissionCalendar"]
logger.debug(f"Made request to leetcode api")
json_ = json.loads(result)
new = {
    datetime.fromtimestamp(int(key)).strftime("%Y-%m-%d"): value
    for key, value in json_.items()
}

# get data and normalize it from github


res = requests.post(
    "https://api.github.com/graphql",
    json={"query": gh_query},
    headers={"Authorization": f"bearer {gh_token}"},
)
# the gh_token could be expired, so we need to handle auth errors
logger.debug(f"Made request to github api")
cal2 = res.json()["data"]["viewer"]["contributionsCollection"]["contributionCalendar"][
    "weeks"
]

# merge all the contributions weeks into one list
cal2 = [item for sublist in cal2 for item in sublist["contributionDays"]]
# convert from {'contributionCount': 0, 'date': '2022-05-08'} to {'2022-05-08': 0}
cal2 = {item["date"]: item["contributionCount"] for item in cal2}


# print min and max value from cal2
# print(f"min: {min(cal2.values())}, max: {max(cal2.values())}")
# print(f"min: {min(new.values())}, max: {max(new.values())}")

# merge and add values when matching keys
for keys in new.keys():
    new[keys] = cal2.get(keys, 0) + new[keys]

for keys in cal2.keys():
    if keys not in new:
        new[keys] = cal2[keys]

# print new sorted by key
# print(sorted(new.items(), key=lambda x: x[0]))

# convert k,v to [k,v] and filter out items that are not needed
new = [[k, v] for k, v in new.items() if v > 0 and k > "2023-01-01"]
# create a json string from new dictionary
new = json.dumps(new)

dump_api_json(new, "calendar_json", engine, inspector)