import logging
import pandas as pd
from pandas import json_normalize
import time
import requests
import json

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("etl.datautilities")

def get_latest_row(table_name, engine, inspector):
    """ Get the latest row processed from metadata table corresponding to the table_name"""
    latest_row = 0
    with engine.connect() as conn:
        result = conn.execute(
            f"SELECT MAX(last_row) AS num_row FROM {table_name}_metadata"
        )
        row = result.fetchone()
        if row is not None and row["num_row"] is not None:
            latest_row = row["num_row"]
    logger.info(f"Latest row processed was: {latest_row}")
    return latest_row


def update_latest_row(table_name, latest_row, engine):
    """ Update the metadata table with the latest row processed and the current time"""
    date = pd.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")
    # insert last row processed into metadata table
    metadata = pd.DataFrame({"last_row": [latest_row], "date": [date]})
    num_rows = metadata.to_sql(f"{table_name}_metadata", engine, if_exists="append", index=False)
    logger.info(
        f"Last row processed was: {latest_row}, Inserted {num_rows} rows into metadata table"
    )

def sqlite2pg(query, table_name, db, engine):
    raw_data = pd.read_sql_query(query, db)
    logger.info(f"Read {raw_data.shape[0]} rows from raw table")
    row_inserted = raw_data.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        chunksize=100000,
        method="multi",
    )
    logger.info(f"Inserted {row_inserted} rows into {table_name} table")
    return raw_data["id"].max() if raw_data.shape[0] > 0 else 0

def normalize_raw(query, table_name, engine, json_column):
    """ Normalize the raw data and insert into the final table"""
    logger.info(f"Reading from raw table: query: {query}")
    raw_data = pd.read_sql_query(query, engine)
    logger.info(f"Read {raw_data.shape[0]} rows from raw table")
    normalized_df = json_normalize(raw_data[json_column])
    normalized_df = pd.concat([raw_data, normalized_df], axis=1)
    normalized_df = normalized_df.drop(columns=[json_column])
    row_inserted = normalized_df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        chunksize=100000,
        method="multi",
    )
    logger.info(f"Inserted {row_inserted} rows into {table_name} table")
    return raw_data["id"].max() if raw_data.shape[0] > 0 else 0

def dump_api_json(json, table_name, engine, inspector):
    """
    Get the lastest key from the table if it exists and add 1 to the key
    Take json and dump into table
    """
    if not inspector.has_table(f"{table_name}"):
        # create table schema
        logger.info(f"Creating table {table_name}")
        # need to make this dynamic by passing in the schema
        # however there is not an easy way to enable primary key in sqlalchemy

        engine.execute(
            f"""CREATE TABLE {table_name} (
            index SERIAL PRIMARY KEY NOT NULL,
            date TIMESTAMP,
            json JSONB
            )"""
        )
    date = pd.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")
    # take json and store it as a json column in postgres
    data = pd.DataFrame({"date": [date], "json": [json]})
    num_rows = data.to_sql(table_name, engine, if_exists="append", index=False)
    logger.debug(f"Inserted {num_rows} rows into {table_name} table")
    return num_rows

def df_dump(df, table_name, engine, schema):
    """
    Get the lastest key from the table if it exists and add 1 to the key
    Take json and dump into table
    """
    num_rows = df.to_sql(table_name, engine, if_exists="append", index=False, dtype=schema)
    logger.debug(f"Inserted {num_rows} rows into {table_name} table")
    return num_rows


def get_ip_data(engine):
    df = pd.read_sql_query(
        """SELECT DISTINCT SUBSTR("request.headers.Cf-Connecting-Ip", 2, LENGTH("request.headers.Cf-Connecting-Ip") - 2) AS IP FROM caddy_fct
        EXCEPT
        SELECT DISTINCT query FROM ip_data""",
        engine,
    )
    logger.info(f"Found {df.shape[0]} unique missing IPs")
    insert_df = pd.DataFrame()
    df["batch"] = df.index // 100
    df = df.groupby("batch").agg({"ip": lambda x: list(x)})
    df = df.reset_index(drop=True)
    
    for row in df.itertuples():
        ip = json.dumps(row.ip)
        try:
            response = requests.post("http://ip-api.com/batch/", data=ip, timeout=12)
            if response.status_code != 200:
                logger.error(f"Error: {response.status_code}")
                break
            data = response.json()
            df = pd.DataFrame.from_records(data)
            if "message" in df.columns:
                df = df.drop(columns=["message"])
            insert_df = pd.concat([insert_df, df])
        except Exception as e:
            logger.error(f"Error: {e}")
            pass
        time.sleep(1)
    
    num_rows = insert_df.to_sql(
        "ip_data",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )
    logger.info(
        f"{num_rows} IP's were added to ip table."
    )