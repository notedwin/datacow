import json
import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

gh_url = "https://api.github.com/graphql"
lc_url = "https://leetcode.com/graphql"
gh_token = os.getenv("GH_TOKEN")
db_url = os.getenv("DATABASE_URL")

duckdb.execute("INSTALL postgres; LOAD postgres;")
duckdb.execute(f"ATTACH 'postgres:{db_url}' AS postgres")


def leetcode_data():
    start = datetime.now().date() - timedelta(days=7)
    end = datetime.now().date()
    lc_query = """query
    {
        matchedUser(username: "notedwin")
        {
            submissionCalendar
        }
    }"""
    res = requests.post(lc_url, json={"query": lc_query})
    return res.json()["data"]["matchedUser"]["submissionCalendar"]


def github_data():
    now_utz = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    # 365 to load everything then down to 7 for cron job
    year_ago = datetime.now() - timedelta(days=7)
    yes_utz = year_ago.strftime("%Y-%m-%dT%H:%M:%SZ")
    gh_query = """query {{
        user(login: "{0}"){{
            contributionsCollection(
                from: "{1}"
                to: "{2}"        
            ){{
                contributionCalendar{{
                    weeks{{
                        contributionDays{{
                            contributionCount
                            date
                        }}
                    }}
                }}
            }}
        }}
    }}"""

    data = {}

    for user in ["notedwin", "notedwin-hznp"]:
        q = gh_query.format(user, yes_utz, now_utz)
        res = requests.post(
            gh_url,
            json={"query": q},
            headers={"Authorization": f"bearer {gh_token}"},
        )
        contribs = res.json()["data"]["user"]["contributionsCollection"][
            "contributionCalendar"
        ]["weeks"]
        for week in contribs:
            for day in week["contributionDays"]:
                date = day["date"]
                count = day["contributionCount"]
                data[date] = data.get(date, 0) + count
    return data


if __name__ == "__main__":
    contributions = {}
    data = json.loads(leetcode_data())
    for date, count in data.items():
        date = datetime.fromtimestamp(int(date)).strftime("%Y-%m-%d")
        print(date, count)
        contributions[date] = count

    data = github_data()
    for date, count in data.items():
        contributions[date] = contributions.get(date, 0) + count

    df = pd.DataFrame(contributions.items(), columns=["Date", "contributions"])

    duckdb.execute(
        """
        INSERT INTO postgres.contribution_calendar
        SELECT date, contributions FROM df
        """
    )
