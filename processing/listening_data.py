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
pd.set_option("display.max_columns", None)



dotenv.load_dotenv()

sp = Spotify(
    auth_manager=SpotifyOAuth(
        scope="user-read-recently-played user-read-playback-state",
        cache_handler=CacheFileHandler(username="notedwin")
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
	"incognito_mode": BOOLEAN
}


for idx in tqdm(range(22)):
    df = pd.read_json(f"~/Downloads/MyData/endsong_{idx}.json")
    df_dump(df, "spotify_listening", engine, schema)

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