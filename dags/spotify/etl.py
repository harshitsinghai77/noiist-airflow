import io
import requests
import datetime
import base64
import logging

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

logger = logging.getLogger(__name__)

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_CLIENT_ID = Variable.get("spotify_client_id")
SPOTIFY_CLIENT_SECRET = Variable.get("spotify_client_secret")

auth_str = "{}:{}".format(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
b64_auth_str = base64.b64encode(auth_str.encode()).decode()
access_token_headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "Basic {}".format(b64_auth_str),
}

# Convert time to Unix timestamp in miliseconds
yesterday = datetime.date.today() - datetime.timedelta(1)
yesterday_unix_timestamp = int(yesterday.strftime("%s"))

session = requests.Session()


def _check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if pd.Series(df["date_added"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True


def get_new_access_token_from_refresh_token(refresh_token: str):
    """Get a new access_token from the refresh token"""
    request_body = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    r = session.post(
        url=SPOTIFY_TOKEN_URL, data=request_body, headers=access_token_headers
    )
    r.raise_for_status()
    resp = r.json()
    return resp["access_token"]


def _get_recently_listened_songs(access_token: str):
    """Fetch and return the recently_listened_songs from Spotify."""

    # Convert time to Unix timestamp in miliseconds
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=access_token),
    }

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    r = session.get(
        "https://api.spotify.com/v1/me/player/recently-played",
        params={"after": yesterday_unix_timestamp, "limit": 50},
        headers=headers,
    )

    r.raise_for_status()
    return r.json()


def get_track_details(id: str, access_token: str):
    """Fetch and return the recently_listened_songs from Spotify."""

    # Convert time to Unix timestamp in miliseconds
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=access_token),
    }

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    r = session.get(
        f"https://api.spotify.com/v1/tracks/{id}",
        headers=headers,
    )

    r.raise_for_status()
    return r.json()["album"]["images"][1]["url"]


def _create_dataframe(recent_songs):
    """Return a pandas dataframe."""
    master_df = []
    for song in recent_songs["items"]:
        track = song["track"]
        tmp_json = {
            "artist": track["artists"][0]["name"],
            "name": track["name"],
            "id": track["id"],
            "url": track["album"]["images"][0]["url"],
            "date_added": song["played_at"],
        }
        master_df.append(tmp_json)
    spotify_dump = pd.DataFrame(master_df)
    return spotify_dump


def run_spotify_etl(
    username: str, refresh_token: str, ds: str, s3_bucket="spotify-users"
):
    logger.info(f"Fetching acess_token for {username}")
    access_token = get_new_access_token_from_refresh_token(refresh_token)

    logger.info(f"Fetching recently_listened_songs from spotify for {username}")
    recent_songs = _get_recently_listened_songs(access_token=access_token)

    spotify_dump = _create_dataframe(recent_songs=recent_songs)
    if spotify_dump.empty:
        print("No songs downloaded for {}. Finishing execution".format(username))
        return
    # _check_if_valid_data(spotify_dump)

    csv_buffer = io.StringIO()
    spotify_dump.to_csv(csv_buffer, index=False)
    data_buffer_binary = io.BytesIO(csv_buffer.getvalue().encode())

    s3_hook = S3Hook(aws_conn_id="s3")
    file_name = f"{username}_{ds}.csv"

    if not s3_hook.check_for_bucket(bucket_name=s3_bucket):
        s3_hook.create_bucket(bucket_name=s3_bucket)

    s3_hook.load_file_obj(
        file_obj=data_buffer_binary,
        bucket_name=s3_bucket,
        key=file_name,
        replace=True,
    )

    logger.info(f"Saved Spotify recent songs file in Minio bucket {username}")
    return file_name
