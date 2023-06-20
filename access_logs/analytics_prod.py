from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

prod = create_engine(os.getenv("DATABASE_URL"))
anal = create_engine(os.getenv("ANAL_URL"))

sql = """
    SELECT
        DATE_TRUNC('day', time_reported)::TEXT AS date,
        COUNT(DISTINCT("request.headers.Cf-Connecting-Ip")) AS count
    FROM caddy_fct
    GROUP BY 1
    ORDER BY 1
"""

# Execute the query on the production database
result = prod.execute(sql)

# Create the aggregate table in the analytics database
anal.execute("CREATE TABLE IF NOT EXISTS agg_table (date VARCHAR PRIMARY KEY, count INTEGER)")

# Insert the query result into the aggregate table
anal.execute("DELETE FROM agg_table")  # Clear existing data if needed
anal.execute("INSERT INTO agg_table (date, count) VALUES (%s, %s)", [(row.date, row.count) for row in result])
