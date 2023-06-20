from sqlalchemy import create_engine
from sqlalchemy import inspect
import logging
from dotenv import load_dotenv
import os

from data_utilities import get_latest_row, update_latest_row, normalize_raw

load_dotenv()
logger = logging.getLogger("etl")
engine = create_engine(os.getenv(key="DATABASE_URL"))
inspector = inspect(engine)


if __name__ == "__main__":
    logger.info("Starting ETL job")
    table_name = "caddy_fct"
    caddy2json_sql = open("caddy2json.sql", "r").read()
    latest_row = get_latest_row(table_name, engine, inspector)
    last_row = normalize_raw(f"{caddy2json_sql} AND id > {latest_row}", table_name, engine, "json_message")
    update_latest_row(table_name, last_row, engine)
    logger.info("ETL job completed")