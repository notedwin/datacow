from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
import os

from data_utilities import get_ip_data

load_dotenv()
logger = logging.getLogger("etl")
engine = create_engine(os.getenv(key="DATABASE_URL"))


if __name__ == "__main__":
    logger.info("Starting ETL job")
    get_ip_data(engine)
    logger.info("ETL job completed")
