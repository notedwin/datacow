from datetime import datetime, timedelta

import duckdb
import pandas as pd
import requests
import structlog
from infisical_client import ClientSettings, GetSecretOptions, InfisicalClient

log = structlog.get_logger()
gh_url = "https://api.github.com/graphql"
lc_url = "https://leetcode.com/graphql"


def leetcode_data():
    lc_query = """query
    {
        matchedUser(username: "notedwin")
        {
            submissionCalendar
        }
    }"""
    res = requests.post(lc_url, json={"query": lc_query})
    return res.json()["data"]["matchedUser"]["submissionCalendar"]


def github_data(gh_token):
    last_date = duckdb.execute(
        """
        SELECT MAX(date) FROM postgres.contribution_calendar
    """
    ).fetchone()[0]
    last_date = datetime.strptime(last_date, "%Y-%m-%d") - timedelta(days=30)
    last_utz = last_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_utz = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

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
        q = gh_query.format(user, last_utz, now_utz)
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


def main():
    client = InfisicalClient(
        ClientSettings(
            access_token="st.0f1fb32c-f2ad-429f-b3b8-bff539975f5f.1910ac88e175bc3ccee1f755d14e44cd.63938e0140a19371329eff95cb2a7cb0",
            site_url="http://spark:8080",
        )
    )

    gh_token = client.getSecret(
        options=GetSecretOptions(
            environment="prod",
            project_id="d62f85ea-2258-45ae-afa2-857ece8d8743",
            secret_name="GH_TOKEN",
        )
    ).secret_value

    db_url = client.getSecret(
        options=GetSecretOptions(
            environment="prod",
            project_id="d62f85ea-2258-45ae-afa2-857ece8d8743",
            secret_name="LOGS_DB",
        )
    ).secret_value

    duckdb.execute("INSTALL postgres; LOAD postgres;")
    duckdb.execute(f"ATTACH IF NOT EXISTS 'postgres:{db_url}' AS postgres")
    contributions = {}
    log.info(f"Github: Fetching contribution calendar update {datetime.now()}")
    # data = json.loads(leetcode_data())
    # for date, count in data.items():
    #     date = datetime.fromtimestamp(int(date)).strftime("%Y-%m-%d")
    #     print(date, count)
    #     contributions[date] = count

    data = github_data(gh_token)
    for date, count in data.items():
        contributions[date] = contributions.get(date, 0) + count

    df = pd.DataFrame(contributions.items(), columns=["Date", "contributions"])

    duckdb.execute(
        """
        INSERT INTO postgres.contribution_calendar
        SELECT date, contributions FROM df
        """
    )

    log.info(f"Github: Inserted {len(data)} new rows")


if __name__ == "__main__":
    main()
