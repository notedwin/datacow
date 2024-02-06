import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

url = "https://api.cloudflare.com/client/v4/graphql/"
api_token = os.getenv("CF_TOKEN")
api_account = os.getenv("CF_ACCOUNT")
db_url = os.getenv("DATABASE_URL")
offset_days = 1
historical_days = 14


duckdb.execute("INSTALL postgres; LOAD postgres;")
duckdb.execute(f"ATTACH 'postgres:{db_url}' AS postgres")


def get_past_date(num_days):
    today = datetime.utcnow().date()
    return today - timedelta(days=num_days)


def get_cf_graphql(start_date, end_date):
    assert start_date <= end_date
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}",
    }
    # The GQL query we would like to use:
    payload = f"""{{"query":
      "query HttpDaily($accountTag: string) {{
        viewer {{
          accounts(
            filter: {{ accountTag: $accountTag }}
          ) {{
            httpRequests1dGroups(
              limit: 40
              filter: $filter
              orderBy: [ date_DESC ]
            ) {{
              dimensions {{
                date
              }}
              uniq {{
                uniques
              }}
              sum {{
                browserMap {{
                  pageViews
                  uaBrowserFamily
                }}
                bytes
                cachedBytes
                cachedRequests
                clientHTTPVersionMap {{
                  clientHTTPProtocol
                  requests
                }}
                clientSSLMap {{
                  clientSSLProtocol
                  requests
                }}
                contentTypeMap {{
                  bytes
                  edgeResponseContentTypeName
                  requests
                }}
                countryMap {{
                  bytes
                  clientCountryName
                  requests
                  threats
                }}
                encryptedBytes
                encryptedRequests
                ipClassMap {{
                  ipType
                  requests
                }}
                pageViews
                requests
                responseStatusMap {{
                  edgeResponseStatus
                  requests
                }}
                threatPathingMap {{
                  requests
                  threatPathingName
                }}
                threats
              }}
            }}
          }}
        }}
      }}",
      "variables": {{
        "accountTag": "{api_account}",
        "filter": {{
          "AND":[
            {{
              "date_geq": "{start_date}"
            }},
            {{
              "date_leq": "{end_date}"
            }}
          ]
        }}
      }}
    }}"""

    r = requests.post(url, data=payload.replace("\n", ""), headers=headers)
    return r.json()


if __name__ == "__main__":
    start_date = get_past_date(offset_days + historical_days)
    end_date = get_past_date(offset_days)

    req = get_cf_graphql(start_date, end_date)

    data = req["data"]["viewer"]["accounts"][0]["httpRequests1dGroups"]

    df = pd.DataFrame(
        [
            {
                "Date": row["dimensions"]["date"],
                "Bytes": row["sum"]["bytes"],
                "CachedBytes": row["sum"]["cachedBytes"],
                "CachedRequests": row["sum"]["cachedRequests"],
                "EncryptedBytes": row["sum"]["encryptedBytes"],
                "EncryptedRequests": row["sum"]["encryptedRequests"],
                "Requests": row["sum"]["requests"],
                "PageViews": row["sum"]["pageViews"],
                "Threats": row["sum"]["threats"],
                "Uniques": row["uniq"]["uniques"],
            }
            for row in data
        ]
    )
    duckdb.execute(
        """
        INSERT INTO postgres.cloudflare_analytics
        SELECT * FROM df
        """
    )
