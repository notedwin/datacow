import json
import os
from datetime import datetime, timedelta

import duckdb
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
    yesterday = datetime.now().date() - timedelta(days=1)
    yes_utz = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    gh_query = """query {
        viewer {
            login
            contributionsCollection {
                startedAt: "{yes_utz}",
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
    res = requests.post(
        gh_url,
        json={"query": gh_query},
        headers={"Authorization": f"bearer {gh_token}"},
    )
    print(res.json())
    return res.json()["data"]["viewer"]["contributionsCollection"][
        "contributionCalendar"
    ]["weeks"]


if __name__ == "__main__":
    contributions = {}
    data = json.loads(leetcode_data())
    for date, count in data.items():
        date = datetime.fromtimestamp(int(date)).strftime("%Y-%m-%d")
        print(date, count)
        contributions[date] = count

    for item in github_data():
        print(item)
        contributions[item["date"]] = (
            contributions.get(item["date"], 0) + item["contributionCount"]
        )

    print(contributions)
    df = duckdb.from_dict(contributions)
    print(df)


# duckdb.execute(
#     """
#     INSERT INTO postgres.cloudflare_analytics
#     SELECT date, contributions FROM contribution_calendar
#     """
# )
