from sqlalchemy import create_engine
import pandas as pd
from pandas import json_normalize
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

engine = create_engine(os.getenv(key="DATABASE_URL"))

df = pd.read_sql_query(
    """
SELECT id,
		message,
		CAST(
			SUBSTRING(
				SUBSTRING(
					message
					FROM 1 FOR POSITION('{' IN message) - 1
				)
				FROM 1 FOR 24
			) AS TIMESTAMP
		) AS time_reported,
		regexp_replace(
			SUBSTRING(
				message
				FROM POSITION('{' IN message)
			),
			'("Cf-Visitor"|"Alt-Svc"|"Sec-Ch-Ua"|"Sec-Ch-Ua-Platform"|"Etag"|"If-None-Match"|"Amp-Cache-Transform"):\s*\[[^]]*\]',
			'\\1:[]',
			'g'
		)::JSON AS json_message
	FROM dockerlogs
	WHERE message LIKE '%%http.log.access.log0%%' 
	AND is_valid_json(regexp_replace(
			SUBSTRING(
				message
				FROM POSITION('{' IN message)
			),
			'("Cf-Visitor"|"Alt-Svc"|"Sec-Ch-Ua"|"Sec-Ch-Ua-Platform"|"Etag"|"If-None-Match"|"Amp-Cache-Transform"):[\s]*\[[^]]*\]',
			'\\1:[]',
			'g'
		)::varchar) AND id > 688974
    """,
    engine,
)

df = json_normalize(df["json_message"])

caddy_fct = pd.read_sql_query(
    """
SELECT * FROM caddy_fct LIMIT 1
    """, engine
)

# list out what columns from the df are not in the caddy_fct table
print(df.columns.difference(caddy_fct.columns))
