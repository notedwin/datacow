import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
from sqlalchemy import inspect
import requests

# from dotenv import load_dotenv

from dagster import MetadataValue, Output, job, op, asset, AssetSelection, Definitions, ScheduleDefinition
from datetime import datetime, timedelta
import os 


# load_dotenv()

engine = create_engine(os.getenv(key="DATABASE_URL"))
inspector = inspect(engine)

caddy2json_sql = open("caddy2json.sql", "r").read()


# @asset
# def hackernews_top_story_ids():
#     """Get top stories from the HackerNews top stories endpoint.

#     API Docs: https://github.com/HackerNews/API#new-top-and-best-stories.
#     """
#     top_story_ids = requests.get(
#         "https://hacker-news.firebaseio.com/v0/topstories.json"
#     ).json()
#     return top_story_ids[:10]


# # asset dependencies can be inferred from parameter names
# @asset
# def hackernews_top_stories(hackernews_top_story_ids):
#     """Get items based on story ids from the HackerNews items endpoint."""
#     results = []
#     for item_id in hackernews_top_story_ids:
#         item = requests.get(
#             f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
#         ).json()
#         results.append(item)

#     df = pd.DataFrame(results)

#     # recorded metadata can be customized
#     metadata = {
#         "num_records": len(df),
#         "preview": MetadataValue.md(df[["title", "by", "url"]].to_markdown()),
#     }

#     return Output(value=df, metadata=metadata)


@op
def get_caddy_data(context):
    latest_row = 0
    # if inspector.has_table(f"caddy_fct_metadata"):
    #     context.log.info(f"caddy_fct_metadata table exists")
    #     df = pd.read_sql_query(
    #         f"select MAX(last_row) as num_row from caddy_fct_metadata", con=engine
    #     )
    #     latest_row = df["num_row"][0]
    context.log.info(f"Latest row processed was: {latest_row}")
    context.log.info(f"Reading from raw table: {caddy2json_sql} AND id > {latest_row}")
    raw_data = pd.read_sql_query(f"{caddy2json_sql} AND id > {latest_row}", con=engine)
    context.log.info(f"Read {raw_data.shape[0]} rows from raw table")


@job
def Caddy2postgres():
    get_caddy_data()

defs = Definitions(
    schedules=[ScheduleDefinition(job=Caddy2postgres, cron_schedule="@daily")]
)