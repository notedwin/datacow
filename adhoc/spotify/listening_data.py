from spotipy import Spotify, SpotifyOAuth, CacheFileHandler
import json

# import dotenv
from dotenv import find_dotenv, load_dotenv

import pandas as pd
from tqdm import tqdm
import os
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.types import TIMESTAMP, BIGINT, VARCHAR, BOOLEAN
import logging
from data_utilities import df_dump
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import ARRAY
import time
import pylast

pd.set_option("display.max_columns", None)

# read in the credentials from doten
load_dotenv(find_dotenv())
API_KEY = os.getenv("LASTFM_API_KEY")
API_SECRET = os.getenv("LASTFM_API_SECRET")
PASSWORD = os.getenv("LAST_FM_PASSWORD")

lastfm_network = pylast.LastFMNetwork(
    api_key=API_KEY,
    api_secret=API_SECRET,
    username="ezfire",
    password_hash=pylast.md5(PASSWORD),
)


def get_recent_tracks(username):
    recent_tracks = lastfm_network.get_user(username).get_recent_tracks(
        limit=10, time_to="1684108650"
    )
    for i, track in enumerate(recent_tracks):
        print(f"{i} {track.playback_date}\t{track.track}")
    return recent_tracks


get_recent_tracks("ezfire")

exit()

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


last_ts = 0

with engine.connect() as conn:
    # the statement below is deprecated in sqlalchemy 2.0?
    result = conn.execute(
        "SELECT CAST(EXTRACT(EPOCH FROM CAST(MAX(ts) AS timestamp) AT TIME ZONE 'CST') AS INT) as max_ FROM spotify_listening"
    )
    row = result.fetchone()
    if row is not None and row["max_"] is not None:
        last_ts = row["max_"]

if last_ts == 0:
    Exception("Did not expect this")

r = sp.current_user_recently_played(limit=50)

print(r)

idx = 0

while r["next"]:
    r = sp.next(r)
    idx += 1
    time.sleep(0.15)

print(idx)
r = pd.json_normalize(r["items"])


print(r)


# r = sp.current_user_recently_played(limit=50, after=one_year_ago)
# print(r['next'])

# r = sp.current_user_recently_played(limit=1)
# # read r into pandas dataframe
# song_features = sp.audio_features(r['items'][0]['track']['uri'])
# # read arrasong_features into polars dataframe
# song_features = pd.DataFrame(song_features)


# def dump_dic_json(d):
# 	with open('listening.json', 'w') as f:
# 		json.dump(d, f)


# def get_all_saved_tracks():
# 	tracks = []
# 	r = sp.current_user_saved_tracks(limit=50)
# 	tracks.append(r)

# 	while r['next']:
# 		r = sp.next(r)
# 		tracks.append(r)
# 		print(r['offset'])
# 		time.sleep(.15)
# 	return tracks


# saved = get_all_saved_tracks()
# dump_dic_json(saved)


d = None

with open("listening.json", "r") as f:
    d = json.load(f)


arr = []
for res in d:
    for item in res["items"]:
        arr.append(
            {
                "added_at": item["added_at"],
                "is_explicit": item["track"]["explicit"],
                "track_name": item["track"]["name"],
                "track_uri": item["track"]["uri"],
                "popularity": item["track"]["popularity"],
                "album_name": item["track"]["album"]["name"],
                "release_date": item["track"]["album"]["release_date"],
                "artists": [x["name"] for x in item["track"]["artists"]],
            }
        )


schema = {
    "added_at": TIMESTAMP,
    "is_explicit": BOOLEAN,
    "track_name": VARCHAR,
    "track_uri": VARCHAR,
    "popularity": BIGINT,
    "album_name": VARCHAR,
    "release_date": VARCHAR,
    "artists": ARRAY(VARCHAR),
}

df_dump(pd.DataFrame(arr), "liked_songs", engine, schema)
