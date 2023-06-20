import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
from dotenv import load_dotenv
import os
import socket
import time
import ssl
import certifi

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("etl")

engine = create_engine(os.getenv(key="DATABASE_URL"))

def get_domain_name(ip: str) -> str:
    context = ssl.create_default_context(cafile=certifi.where())
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    # hostname = socket.gethostbyaddr(ip)[0]
    with socket.create_connection((ip, 443)) as sock:
        with context.wrap_socket(sock, server_hostname=ip) as sslsock:
            cert = sslsock.getpeercert()
            subject = dict(x[0] for x in cert['subject'])
            #F00 DEBUG: print(f"Certificate subject: {subject['commonName']}")
            return subject['commonName']

def check_port(ip: str) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(.25)
    try:
        s.connect((ip, 443))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except:
        return False
    finally:
        s.close()

def main():
    logger.info("Starting ETL job")
    # get todays date
    df = pd.read_sql_query(
        """SELECT DISTINCT
    SUBSTR("request.headers.Cf-Connecting-Ip", 2, LENGTH("request.headers.Cf-Connecting-Ip") - 2) AS ip
    FROM caddy_fct 
    WHERE time_reported::date > '2023-04-25'::date""", engine)



    arr = []

    for index, row in df.iterrows():
        ip = row["ip"]
        if check_port(ip):
            #F00 DEBUG: print(f"Finding domain for: {ip}")
            try:
                domain = get_domain_name(ip)
                arr.append({"ip": ip, "domain": domain})
                #F00 DEBUG: print(f"IP: {ip} is accociated with {domain}")
            except:
                continue

    print(pd.DataFrame(arr))

    df.to_sql(
        "ip2domain",
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )

    logger.info("ETL job completed")


if __name__ == "__main__":
    main()