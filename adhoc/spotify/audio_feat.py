from spotipy import Spotify, SpotifyOAuth, CacheFileHandler
import json
import dotenv
import pandas as pd
from tqdm import tqdm
import os
from sqlalchemy import create_engine
from sqlalchemy.types import TIMESTAMP, BIGINT, VARCHAR, BOOLEAN, DECIMAL
import logging
from data_utilities import df_dump
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import ARRAY
import time

pd.set_option("display.max_columns", None)

dotenv.load_dotenv()

sp = Spotify(
    auth_manager=SpotifyOAuth(
        scope="user-read-recently-played user-read-playback-state",
        cache_handler=CacheFileHandler(username="notedwin"),
    )
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("spotify")
engine = create_engine(os.getenv(key="DATABASE_URL"))

#
# r = sp.audio_analysis("spotify:track:0SRLhugsMVdQdQiZpMAYFL")
# print(r)

df = pd.read_sql_query(
    """SELECT DISTINCT spotify_track_uri as uri FROM spotify_listening WHERE spotify_track_uri IS NOT NULL AND spotify_track_uri NOT IN (SELECT uri FROM audio_features)""",  # noqa: E501
    engine,
)

df["batch"] = df.index // 100
df = df.groupby("batch").agg({"uri": lambda x: list(x)})
df = df.reset_index(drop=True)

schema = {
    "danceability": DECIMAL,
    "energy": DECIMAL,
    "key": BIGINT,
    "loudness": DECIMAL,
    "mode": BIGINT,
    "speechiness": DECIMAL,
    "acousticness": DECIMAL,
    "instrumentalness": DECIMAL,
    "liveness": DECIMAL,
    "valence": DECIMAL,
    "tempo": DECIMAL,
    "type": VARCHAR,
    "id": VARCHAR,
    "uri": VARCHAR,
    "track_href": VARCHAR,
    "analysis_url": VARCHAR,
    "duration_ms": BIGINT,
    "time_signature": BIGINT,
}

for uris in tqdm(df["uri"]):
    r = sp.audio_features(tracks=uris)
    # filter none objects
    r = [item for item in r if item]
    # print(r)
    df = pd.DataFrame(r)
    df_dump(df, "audio_features", engine, schema)
    time.sleep(0.15)
