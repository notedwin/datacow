from spotipy import Spotify, SpotifyOAuth, CacheFileHandler
import json
import dotenv
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
inspector = inspect(engine)

schema = {
	"ts": TIMESTAMP,
	"username": VARCHAR,
	"platform": VARCHAR,
	"ms_played": BIGINT,
	"conn_country": VARCHAR,
	"ip_addr_decrypted": VARCHAR,
	"user_agent_decrypted": VARCHAR,
	"master_metadata_track_name": VARCHAR,
	"master_metadata_album_artist_name": VARCHAR,
	"master_metadata_album_album_name": VARCHAR,
	"spotify_track_uri": VARCHAR,
	"episode_name": VARCHAR,
	"episode_show_name": VARCHAR,
	"spotify_episode_uri": VARCHAR,
	"reason_start": VARCHAR,
	"reason_end": VARCHAR,
	"shuffle": BOOLEAN,
	"skipped": BOOLEAN,
	"offline": BOOLEAN,
	"offline_timestamp": BIGINT,
	"incognito_mode": BOOLEAN,
}


# for idx in tqdm(range(22)):
#     df = pd.read_json(f"~/Downloads/MyData/endsong_{idx}.json")
#     df_dump(df, "spotify_listening", engine, schema)

# @app.route("/spotify/last_year")
# def last_year():

#     one_year_ago = datetime.now() - timedelta(days=365)
#     one_year_ago = int(one_year_ago.timestamp())


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

with open('listening.json', 'r') as f:
	d =  json.load(f)


arr = []
for res in d:
	for item in res['items']:
		arr.append({
			"added_at": item["added_at"],
			"is_explicit": item["track"]["explicit"],
			"track_name": item["track"]["name"],
			"track_uri": item["track"]["uri"],
			"popularity": item["track"]["popularity"],
			"album_name": item["track"]["album"]["name"],
			"release_date": item["track"]["album"]["release_date"],
			"artists": [x["name"] for x in item["track"]["artists"]],
		})


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
