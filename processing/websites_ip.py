import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
from dotenv import load_dotenv
import os
import socket
import time

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("etl")

engine = create_engine(os.getenv(key="DATABASE_URL"))



def isOpen(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    try:
            s.connect((ip, int(port)))
            s.shutdown(socket.SHUT_RDWR)
            return True
    except:
            return False
    finally:
            s.close()

if __name__ == "__main__":
    logger.info("Starting ETL job")
    # get distinct ip from today
    df = pd.read_sql_query("""SELECT DISTINCT
    SUBSTR("request.headers.Cf-Connecting-Ip", 2, LENGTH("request.headers.Cf-Connecting-Ip") - 2) AS ip
    FROM caddy_fct 
    WHERE time_reported::date > '2023-04-10'::date""", engine)

    # check if port 80 is open
    for index, row in df.iterrows():
        ip = row["ip"]
        if isOpen(ip, 443):
            logger.info(f"{ip} is open")
    
    logger.info("ETL job completed")

