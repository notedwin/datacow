import duckdb

CON = duckdb.connect("file.db")
# startup script to use aws cli to get env variables

CON.execute(
    """
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_region='us-east-1';
    SET s3_access_key_id='';
    SET s3_secret_access_key='';
    SET enable_object_cache=true;
"""
)

data = CON.query(
    """
        SELECT 
            Start_Lon,
            Start_Lat,
            End_Lon,
            End_Lat 
        FROM read_parquet('s3://nyc-tlc/trip data/yellow_tripdata_2009-01.parquet') LIMIT 1;
    """
).to_df()

print(data)
