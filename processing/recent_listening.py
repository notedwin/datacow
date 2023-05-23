from datetime import datetime
from spotipy import Spotify, SpotifyOAuth, CacheFileHandler
import json
import requests
import os
from sqlalchemy import create_engine
from sqlalchemy import inspect
import logging
import dotenv
import base64
from sqlalchemy.types import JSON, TIMESTAMP, Inter
from data_utilities import dump_api_json

dotenv.load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("spotify")
engine = create_engine(os.getenv(key="DATABASE_URL"))
inspector = inspect(engine)


sp = Spotify(
    auth_manager=SpotifyOAuth(
        scope="user-read-recently-played user-read-playback-state",
        cache_handler=CacheFileHandler(username="notedwin")
    )
)

def get_recent_tracks():
    # get currently playing track
    r = sp.current_user_playing_track()
    logger.info(f"Getting current playing track")
    if r:
        logger.info(f"Currently playing track: {r['item']['name']}")
        # get image from url and convert to base64
        im = requests.get(r['item']['album']['images'][0]['url'])
        im = im.content
        im = base64.b64encode(im)
        # convert bytes to string
        d = {
        'name': r['item']['name'],
        'artist': r['item']['artists'][0]['name'],
        'album': r['item']['album']['name'],
        'image': im.decode('utf-8'),
        'url': r['item']['external_urls']['spotify'],
        }
        return d
    else:
        r = sp.current_user_recently_played(limit=1)
        logger.info(f"Recently played track: {r['items'][0]['track']['name']}")
        im = requests.get(r['item']['album']['images'][0]['url'])
        im = im.content
        im = base64.b64encode(im)
        
        d = {
        'name': r['items'][0]['track']['name'],
        'artist': r['items'][0]['track']['artists'][0]['name'],
        'album': r['items'][0]['track']['album']['name'],
        'image': im.decode('utf-8'),
        'url': r['items'][0]['track']['external_urls']['spotify']
        }
        return d
    return None

# results = sp.current_user_saved_tracks()
# for idx, item in enumerate(results['items']):
#     track = item['track']
#     print(idx, track['artists'][0]['name'], " – ", track['name'])

cur = get_recent_tracks()
cur = json.dumps(cur)

# schema = {
#     "index": Integer,
#     "date": TIMESTAMP,
#     "json": JSON,
# }

dump_api_json(cur, "spotify_json", engine, inspector)
# pull 50 most recent tracks with an offset of last time we pulled