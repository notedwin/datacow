from sqlalchemy import create_engine
from sqlalchemy import inspect
import os
import sqlite3
from data_utilities import sqlite2pg, get_latest_row, update_latest_row
from dotenv import load_dotenv
load_dotenv()

engine = create_engine(os.getenv(key="DATABASE_URL"))
inspector = inspect(engine)

db = sqlite3.connect(
    os.getenv(key="SQLITE_DB")
)


def table2postgres(table_name):
    latest_row = get_latest_row(table_name, engine, inspector)
    last_row = sqlite2pg(f"SELECT * FROM {table_name} WHERE id > {latest_row}", table_name, db, engine)
    update_latest_row(table_name, last_row, engine)
    

def main():
    table2postgres("eventmodel")
    # table2postgres("bucketmodel")


if __name__ == "__main__":
    main()
